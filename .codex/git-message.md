Generate a commit message for currently staged files.

Rules:
1. Inspect staged diff only.
2. Choose a subject prefix that matches the staged change type:
   - `(feature)` for new user-facing capability
   - `(refactor)` for internal code restructuring with no intended behavior change
   - `(fix)` for bug fixes
   - `(docs)` for documentation-only changes
   - `(chore)` for maintenance, tooling, config, dependency/runtime upgrades
   - If multiple types exist, choose the dominant change type by impact.
3. Return output in this exact format:
   - A single fenced code block
   - First line: commit subject
   - Blank line
   - Bullet list body lines
4. Do not include any text before or after the code block.
