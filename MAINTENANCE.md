# Maintenance

The following document includes information how to release cpp-rust-driver and
other information / procedures useful for maintainers. It is heavily inspired by https://github.com/scylladb/scylla-rust-driver/blob/v1.2.0/MAINTENANCE.md.

## Documentation

TODO (no docs written yet)
Note: Due to API compatibility, cpp-driver docs apply to cpp-rust-driver in general. Though, some discrepancies should be well-documented in cpp-rust-driver (some of them are already mentioned in `cassandra.h`).

## Version bump commit

There are two places to be addressed when bumping the version of the driver.
1. `scylla-rust-wrapper/Cargo.toml` -> `version` field should be adjusted
2. `include/cassandra.h` -> there are `CASS_VERSION_[MAJOR/MINOR/PATCH/SUFFIX]` definitions which should be adjusted accordingly

## Releasing

IMPORTANT: Read this whole document before attempting to do a release.

Prerequisites:
- Have write access to GitHub repo (so that you are able to push to master and create a new branch).
- Decide what the new version should be.

### Releasing a new version

1. Checkout `master`.
2. Create some new branch (which you'll later push to your fork).
3. (Optional, but recommended) Create a commit with changes applied by running `cargo update`. This will update the dependencies in `Cargo.lock` file, which we keep track of in the repository.
4. Create commit that bumps version numbers of the driver (see `Version bump commit` section in this document).
5. Prepare release notes (see the template at the bottom of this document).
6. (Optional, but recommended) Push the branch to your fork and create the PR to `master` so that another maintainer can review your work.
Description of this PR should consist of just release notes. **NOTE: Preferably merge this PR with CLI as described in next step. If you really want to use GitHub UI only use REBASE merge.** This is to make all commits appear on the version branch, without any merge commits / squash commits.
7. Checkout `master` and fast-forward merge your changes (`git merge --ff-only your_branch`).
8. Create a new tag (e.g. `git tag -a v1.2.0 -m "Release 1.2.0"`).
9. Push `master` and the new tag, preferably using atomic push (e.g. `git push --atomic origin master v1.2.0`).
10. Remove `version` file if present. This file contains version information (`X.Y.Z.<DATE>.<COMMIT_ID>`) and should be generated from scratch when releasing new version. The file will be re-generated automatically by instructions described in the next steps.
11. Build RPM packages (e.g. `./dist/redhat/build_rpm.sh --target rocky-8-x86_64`) - see `Build rpm package` section of README.md for more details. We don't have a strict policy which distribution versions to choose during release. As an example, during release 0.4.0, we provided packages for Fedora40, Fedora41 and Rocky9. This would correspond to two latest Fedora versions and latest Rocky version at the time. Ideally, the chosen versions should be in-sync with distribution versions we use in our CI.
12. Build DEB packages (e.g. .`/dist/debian/build_deb.sh --target jammy`) - see `Build deb package` section of README.md for more details. Again, we don't have a strict policy regarding the chosen distribution versions. Looking at the previous releases, we always built packages for `jammy` and `noble`.
13. Go to https://github.com/scylladb/cpp-rust-driver/releases , click the `Draft new release` button and follow the procedure to create a new release on GitHub. Use the release notes as its description. Be sure to upload built packages from the previous steps - see previous releases to check which files are included.
14. (Mandatory for major / minor release, optional for patch release) Publish a post on the forum:
    - Go to [Release notes](https://forum.scylladb.com/c/scylladb-release-notes/18) section.
    - Click "New Topic".
    - Title should be `[RELEASE] ScyllaDB CPP-over-Rust Driver <version>`, e.g. `[RELEASE] ScyllaDB CPP-over-Rust Driver 0.5.0`
    - Tags: `release`, `drivers`, `driver-release`.
    - Content of the post should just be release notes.
    - Click "Create Topic"
    - Posts in "Release notes" section often need additional confirmation. You can write to current forum admin to expedite this.

You're done!

## Writing release notes

PR titles are written for maintainers, but release notes entries are written for users.
It means that they should not be the same - and if they are then they are probably
not good as either PR titles or release notes entries.

For that reason we hand-write our release notes, and link to relevant PRs in the entries.

Some old release notes that you can take inspiration from when writing new ones:
- https://github.com/scylladb/cpp-rust-driver/releases/tag/v0.2.0
- https://github.com/scylladb/cpp-rust-driver/releases/tag/v0.3.0
- https://github.com/scylladb/cpp-rust-driver/releases/tag/v0.4.0

The template for release notes can be found in the section below.

Guidelines on how to write release notes:

- Go over all the PRs / commits since previous release. Usually: `git log --first-parent` to see
  merge commits and commits that are directly on a branch. You can also try filtering
  merged PRs on GitHub by merge date, but it's cumbersome. Since 0.5 we try to assign each merged PR to a milestone,
  which should make this process much easier - you can just go over e.g. https://github.com/scylladb/cpp-rust-driver/milestone/6?closed=1

- Items in release notes will usually correspond 1:1 to PRs / commits - but not always. It is possible that
  some functionality that should be a single item on the list is split over multiple PRs.
  It is also possible that single PR will be mentioned in two items.

- Release notes should contain all items since previous (in terms of semver) version released at the time. Items that were backported will be
  duplicated between release notes for at least 2 different versions that way. This is fine, the backport should just be marked as such in release notes.

- Release notes should contain a table with the number of non-backported commits per contributor.
  Depending on the situation you may generate it with `git shortlog --no-merges $(git merge-base master previous_version_tag)..HEAD -s -n` and manually subtracting
  backported commits, or just `git shortlog --no-merges previous_version_tag..HEAD -s -n` .
  If it is too much work, or you can't figure out how to calculate it in particular situation, you can skip it.
  This table should not count version bump commits - subtract them from your
  row if you already created them.

- Remember to update the amount of crate downloads and GitHub stars!


## Release notes template

PR numbers in the list are random, they are just here to emphasize that entries
should contain links to relevant PR / PRs.

```
The ScyllaDB team is pleased to announce ScyllaDB CPP Rust Driver X.Y.Z, an API-compatible rewrite of https://github.com/scylladb/cpp-driver as a wrapper for the Rust driver. It will fully replace the CPP driver, which is already reaching its End of Life.
**The current driver version should be considered Beta**.

Some minor features still need to be included. See Limitations section in README.md.

The underlying Rust driver used version: A.B.C.

## Changes

**New features / enhancements:**
- Some new feature 1 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))
- Some new feature 2 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))

**Bug fixes:**
- Some bugfix 1 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))
- Some bugfix 2 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))

**Documentation:**
- Doc update 1 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))
- Doc update 2 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))

**CI / developer tool improvements:**
- Update 1 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))
- Update 2 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))

**Others:**
- Update 1 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))
- Update 2 ([297](https://github.com/scylladb/cpp-rust-driver/pull/297))

Congrats to all contributors and thanks everyone for using our driver!

----------

The source code of the driver can be found here:
- [https://github.com/scylladb/cpp-rust-driver](https://github.com/scylladb/cpp-rust-driver)

Contributions are most welcome!

Thank you for your attention, please do not hesitate to contact us if you have any questions, issues, feature requests, or are simply interested in our driver!

Contributors since the last release:

| commits | author            |
|---------|-------------------|
| 45      | Lucille Perkins   |
| 34      | Rachel Burton     |
| 17      | Mercedes Marks    |

```

