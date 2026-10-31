# Third-party notices

AI Control Plane's original work is licensed under [Apache-2.0](LICENSE).
Dependencies retain their own copyright, license and attribution terms. This
file identifies the pinned dependencies; the linked directories contain exact
upstream license and notice files. These copies do not relicense dependencies.

## Go modules

| Module | Version | Terms | Preserved texts |
| --- | --- | --- | --- |
| `github.com/santhosh-tekuri/jsonschema/v6` | `v6.0.2` | Apache-2.0 | [LICENSE](third_party/licenses/github.com/santhosh-tekuri/jsonschema/v6/v6.0.2/LICENSE) |
| `modernc.org/sqlite` | `v1.46.2` | BSD-3-Clause; upstream SQLite public domain | [LICENSE, SQLITE-LICENSE, AUTHORS](third_party/licenses/modernc.org/sqlite/v1.46.2/) |
| `github.com/dustin/go-humanize` | `v1.0.1` | MIT | [LICENSE](third_party/licenses/github.com/dustin/go-humanize/v1.0.1/LICENSE) |
| `github.com/google/uuid` | `v1.6.0` | BSD-3-Clause | [LICENSE](third_party/licenses/github.com/google/uuid/v1.6.0/LICENSE) |
| `github.com/mattn/go-isatty` | `v0.0.20` | MIT | [LICENSE](third_party/licenses/github.com/mattn/go-isatty/v0.0.20/LICENSE) |
| `github.com/ncruces/go-strftime` | `v1.0.0` | MIT | [LICENSE](third_party/licenses/github.com/ncruces/go-strftime/v1.0.0/LICENSE) |
| `github.com/remyoudompheng/bigfft` | `v0.0.0-20230129092748-24d4a6f8daec` | BSD-3-Clause | [LICENSE](third_party/licenses/github.com/remyoudompheng/bigfft/v0.0.0-20230129092748-24d4a6f8daec/LICENSE) |
| `golang.org/x/sys` | `v0.42.0` | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org/x/sys/v0.42.0/LICENSE) |
| `golang.org/x/text` | `v0.14.0` | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org/x/text/v0.14.0/LICENSE) |
| `modernc.org/libc` | `v1.70.0` | BSD-3-Clause plus upstream component terms | [LICENSE, third-party notices, AUTHORS](third_party/licenses/modernc.org/libc/v1.70.0/) |
| `modernc.org/mathutil` | `v1.7.1` | BSD-3-Clause, including mersenne notices | [LICENSE, mersenne/LICENSE, AUTHORS](third_party/licenses/modernc.org/mathutil/v1.7.1/) |
| `modernc.org/memory` | `v1.11.0` | BSD-3-Clause plus Go and mmap-go notices | [LICENSE, LICENSE-GO, LICENSE-MMAP-GO, AUTHORS](third_party/licenses/modernc.org/memory/v1.11.0/) |

The inventory covers all twelve requirements in `go.mod`, including indirect
modules. The Linux/amd64 build/test closure imports ten of them. `go-isatty` and
`go-strftime` are declared requirements but were not imported in that closure.
They remain listed so declared dependencies are not silently omitted.

`modernc.org/libc`'s preserved third-party notice includes musl, Go, go-netdb
and NixOS/nixpkgs attribution, including musl's component notices and exceptions.
Upstream modules' test-only fixtures, logos and unused platform-specific assets
are not copied into this repository or bundled with the preview. Their presence in
a module download does not make them project-owned Apache-2.0 material.

## Go runtime

The local preview build uses Go 1.26.8. Its BSD-3-Clause text is preserved in
[Go LICENSE](third_party/licenses/go/go1.26.8/LICENSE). Keep applicable Go and
dependency notices with any redistributed compiled binary. Recheck the exact
build inputs when changing toolchains, modules, platform or packaging.

## External tools

The pinned GitHub Actions [checkout license](https://raw.githubusercontent.com/actions/checkout/3d3c42e5aac5ba805825da76410c181273ba90b1/LICENSE)
and [setup-go license](https://raw.githubusercontent.com/actions/setup-go/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e/LICENSE)
are MIT. The workflow references them; their code and bundled dependencies are
not redistributed in this repository.

Git, Go tooling, Python and Codex are external build/development/client tools.
No copies of their executables or client SDKs are shipped. Their own terms
continue to apply. Linked external material retains its original terms.

## Redistribution

For source distributions, retain the project `LICENSE`, this index, the complete
`third_party/licenses/` directory and relevant original notices. If vendoring
source, preserve its headers and additional notices as well.

For binary distributions, ship those license materials alongside the binary.
Keep MIT/BSD copyright notices, conditions and disclaimers, and satisfy any
applicable Apache-2.0 notice and modified-file requirements. No binary release
is created by this licensing change.

No upstream Apache `NOTICE` file was found in the reviewed dependency inputs.
A separate project `NOTICE` is not added. The existing third-party notices are
retained here and in their original files. Reassess this if new material brings
an applicable `NOTICE` obligation.
