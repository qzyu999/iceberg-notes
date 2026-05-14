- rewrite:
    - function needs to accept argument for a filter (partition or not, just pointing to specific data)
    - from those selected files (either subset or whole table)
    - group them into groups (by partition)
    - for each partition, group files into groups (by size)
    - for each group, rewrite them into a new file
    - commit the changes

- next steps:
    - ensure java implementation matches the compaction_exploration.ipynb
    - create some pseudocode, maybe some actual code in the dev branch to show how to use the rewrite function
    - create reply in issue, saying this is the understanding, this is the high-level plan, and ask for feedback

- nuances:
    - when rewriting files, they may have data files from old partition specs due to schema evolution
        - discussed here: /Users/jaredyu/Desktop/open_source/issue-1092/nuances_pt1.md
    - the expectedOutputFiles function in /Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java focuses on the remainder problem e.g., if there's a 1.01GB file, can we take that 100MB file and squash it into some other of the files to be rewritten to avoid writing an 11th file of 100MB?