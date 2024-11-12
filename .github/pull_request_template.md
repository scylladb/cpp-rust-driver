## Pre-review checklist

<!--
    Make sure you took care of the issues on the list.
    Put 'x' into those boxes which apply.
    You can also create the PR now and click on all relevant checkboxes.
-->

- [ ] I have split my patch into logically separate commits.
- [ ] All commit messages clearly explain what they change and why.
- [ ] PR description sums up the changes and reasons why they should be introduced.
- [ ] I have implemented Rust unit tests for the features/changes introduced.
- [ ] I have enabled appropriate tests in `.github/workflows/build.yml` in `gtest_filter`.
- [ ] I have enabled appropriate tests in `.github/workflows/cassandra.yml` in `gtest_filter`.