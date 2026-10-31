package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ai-control-plane/src/internal/app"
	"ai-control-plane/src/internal/mcp"
	"ai-control-plane/src/internal/orchestrator"
	"ai-control-plane/src/internal/statestore"
	"golang.org/x/sys/unix"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--fake-worker" {
		if err := orchestrator.FakeWorker(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// This process is the local Orchestrator. StateStore does not default missing
	// callers to controller authority.
	ctx = statestore.WithMutationActor(ctx, "ORCHESTRATOR")
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if !app.LogNumericFailure(ctx, slog.New(slog.NewJSONHandler(os.Stderr, nil)), err) {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

// Inherited stdio uses blocking os.Files. Register nonblocking duplicates with
// Go's poller so Close can interrupt pending MCP reads and writes on shutdown.
func mcpStdioFile(file *os.File) (*os.File, error) {
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		return nil, errors.Join(err, unix.Close(fd))
	}
	return os.NewFile(uintptr(fd), file.Name()), nil
}

func run(ctx context.Context, args []string, output io.Writer, options ...statestore.OpenOptions) (err error) {
	flags := flag.NewFlagSet("ai-control-plane", flag.ContinueOnError)
	flags.SetOutput(output)
	dataDir := flags.String("data-dir", "", "installation data directory on a local Linux filesystem")
	mcpMode := flags.Bool("mcp", false, "serve external workflows over MCP stdio")
	fakeRun := flags.String("fake-run", "", "create a deterministic fake workflow with this Run ID")
	resume := flags.String("resume", "", "resume a persisted fake workflow without redispatching ambiguous releases")
	goal := flags.String("goal", "", "goal of the deterministic fake workload")
	budget := flags.Int64("normal-tokens", 500000, "normal WORK budget; requires explicit confirmation")
	approval := flags.String("approve-budget", "", "actual explicit human response approving the displayed normal WORK budget")
	increment := flags.Int64("increase-normal-tokens", 0, "explicit positive increment for a resumed fake Run")
	increaseApproval := flags.String("approve-increase", "", "actual human response approving the exact normal-budget increment")
	interaction := flags.String("interaction-id", "", "unique actual human interaction identity for an increase or Hard response")
	hardRequest := flags.String("hard-request", "", "persisted presented Hard proposal to decide on resume")
	hardDecision := flags.String("hard-decision", "", "APPROVE, DENY or TIMEOUT for the persisted proposal")
	humanResponse := flags.String("human-response", "", "actual human response to the exact Hard proposal")
	deadline := flags.Duration("hard-window", time.Hour, "immutable fake workflow Hard window")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	store, err := app.Start(ctx, *dataDir, options...)
	if err != nil {
		return fmt.Errorf("startup failed: %w", err)
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()
	if *mcpMode {
		if *fakeRun != "" || *resume != "" || *increment != 0 || *hardRequest != "" {
			return fmt.Errorf("MCP mode cannot combine with fake CLI workflow operations")
		}
		runtime, runtimeErr := orchestrator.New(store)
		if runtimeErr != nil {
			return runtimeErr
		}
		input, inputErr := mcpStdioFile(os.Stdin)
		if inputErr != nil {
			return inputErr
		}
		files := []*os.File{input}
		closeIO := func() (err error) {
			for _, file := range files {
				if closeErr := file.Close(); !errors.Is(closeErr, os.ErrClosed) {
					err = errors.Join(err, closeErr)
				}
			}
			return err
		}
		if file, ok := output.(*os.File); ok {
			pollable, outputErr := mcpStdioFile(file)
			if outputErr != nil {
				return errors.Join(outputErr, closeIO())
			}
			files = append(files, pollable)
			output = pollable
		}
		done := make(chan struct{})
		stopped := make(chan struct{})
		var ioCloseErr error
		defer func() {
			close(done)
			<-stopped
			err = errors.Join(err, ioCloseErr, closeIO())
		}()
		go func() {
			defer close(stopped)
			select {
			case <-ctx.Done():
				ioCloseErr = closeIO()
			case <-done:
			}
		}()
		err = mcp.Serve(ctx, runtime, input, output)
		if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, os.ErrClosed)) {
			return nil
		}
		return err
	}
	info := store.Info()
	if _, err := fmt.Fprintf(output, "Persistence ready: %s (SQLite %s, schema %d)\n", info.Path, info.SQLiteVersion, info.SchemaVersion); err != nil {
		return err
	}
	if *fakeRun != "" || *resume != "" {
		if *fakeRun != "" && *resume != "" {
			return fmt.Errorf("choose fake-run or resume")
		}
		runtime, err := orchestrator.New(store)
		if err != nil {
			return err
		}
		id := *resume
		if *fakeRun != "" {
			if *approval == "" {
				return fmt.Errorf("explicit --approve-budget response is required")
			}
			prompt := fmt.Sprintf("Confirm %d normal WORK tokens; contingency is additional floor(normal/10).", *budget)
			if _, err = fmt.Fprintln(output, prompt); err != nil {
				return err
			}
			at := time.Now().UTC()
			id = *fakeRun
			spec := statestore.WorkflowSpec{RunID: id, Goal: *goal, ContentIdentity: fmt.Sprintf("content-%x", sha256.Sum256([]byte(*goal))), StartedAt: at.Format(time.RFC3339Nano), HardDeadline: at.Add(*deadline).Format(time.RFC3339Nano), NormalTokens: *budget, Confirmation: statestore.HumanBudgetApproval{ApprovalID: fmt.Sprintf("approval-%x", sha256.Sum256([]byte(id))), InteractionID: fmt.Sprintf("interaction-%x", sha256.Sum256([]byte(id))), Prompt: prompt, Response: *approval, Decision: "APPROVE"}}
			if err = runtime.Initialize(ctx, spec); err != nil {
				return err
			}
		} else {
			w, err := store.GetWorkflow(ctx, id)
			if err != nil {
				return err
			}
			if w.Status == "RUNNING" {
				if err = runtime.Initialize(ctx, w.Spec); err != nil {
					return err
				}
			}
		}
		if *increment != 0 {
			if *resume == "" || *increaseApproval == "" || *interaction == "" {
				return fmt.Errorf("increase requires resume, approve-increase and interaction-id")
			}
			h := statestore.HumanBudgetApproval{ApprovalID: fmt.Sprintf("approval-%x", sha256.Sum256([]byte(*interaction))), InteractionID: *interaction, Prompt: fmt.Sprintf("Approve %d additional normal WORK tokens for this Run?", *increment), Response: *increaseApproval, Decision: "APPROVE"}
			if _, err = runtime.IncreaseBudget(ctx, id, *increment, h); err != nil {
				return err
			}
		}
		if *hardRequest != "" {
			if *resume == "" {
				return fmt.Errorf("Hard decision requires resume")
			}
			var h *statestore.HumanBudgetApproval
			if *hardDecision != "TIMEOUT" {
				if (*hardDecision != "APPROVE" && *hardDecision != "DENY") || *interaction == "" || *humanResponse == "" {
					return fmt.Errorf("Hard response requires APPROVE/DENY, interaction-id and human-response")
				}
				proposal, _, err := store.WorkflowHardRequest(ctx, id, *hardRequest)
				if err != nil {
					return err
				}
				h = &statestore.HumanBudgetApproval{ApprovalID: fmt.Sprintf("approval-%x", sha256.Sum256([]byte(*interaction))), InteractionID: *interaction, Prompt: orchestrator.HardQuestion(proposal.Grant.Amount), Response: *humanResponse, Decision: *hardDecision}
			}
			if _, err = runtime.DecideHardProposal(ctx, id, *hardRequest, h); err != nil {
				return err
			}
		}
		w, err := runtime.Run(ctx, id)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(struct {
			RunID, Status        string
			Terminal             statestore.ProofReference
			AccountingIncomplete bool
			AccountingStatus     string
		}{id, w.Status, w.Terminal, w.AccountingIncomplete, w.AccountingStatus})
	}
	<-ctx.Done()
	return nil
}
