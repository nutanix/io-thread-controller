# Commit messages

- Commit title should start with a capital letter.
- If you used AI, state so in the commit message, e.g.:
  "AI-generated: everything except commit message".

# Unit Tests

- Unit test name should have the `test_` prefix.
- Unit test documentation should explain what this does test tests, e.g.:
  "Tests that foo does such and such."

# Use of `std::path::Path`

- Avoid using `std::path::Path` in favor of `crate::path::Path`.
- If `std::path::Path` is unavoidable, use the type with the full path, e.g.:
  `fn foo(path: impl AsRef<std::path::Path>)`.
