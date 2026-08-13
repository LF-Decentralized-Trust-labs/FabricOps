# Contributing

Thanks for your interest in FabricOps.

FabricOps is an LF Decentralized Trust Lab. Contributions should follow the
project's Apache 2.0 license, DCO sign-off requirement, and normal pull request
review flow.

## Developer Certificate of Origin

All commits must include a DCO sign-off trailer:

```text
Signed-off-by: Your Name <your.email@example.com>
```

The easiest way to add it is:

```bash
git commit -s -m "Describe your change"
```

## Development Flow
1. Create a fork of this repo.
2. Create a feature branch.
3. Make focused changes with tests or documentation updates where appropriate.
4. Run the relevant local checks.
5. Open a pull request for review.

For most code changes, start with:

```bash
make test
make lint
```

For changes that affect Kubernetes installation or runtime behavior, also run
the relevant e2e or chart tests described in the README.

## Code of Conduct

FabricOps follows the LF Decentralized Trust
[Code of Conduct](https://lf-decentralized-trust.github.io/governance/governing-documents/code-of-conduct.html#code-of-conduct).
