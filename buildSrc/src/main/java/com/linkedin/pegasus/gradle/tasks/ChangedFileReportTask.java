package com.linkedin.pegasus.gradle.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.Specs;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.FileChange;
import org.gradle.work.InputChanges;


public class ChangedFileReportTask extends DefaultTask
{
    private final Collection<String> _needCheckinFiles = new ArrayList<>();

    private FileCollection _idlFiles = getProject().files();
    private FileCollection _snapshotFiles = getProject().files();

    public ChangedFileReportTask()
    {
        //with Gradle 6.0, Declaring an incremental task without outputs is not allowed.
        getOutputs().upToDateWhen(Specs.satisfyNone());
    }

    // DataHub Note - updated for InputChanges
    @TaskAction
    public void checkFilesForChanges(InputChanges inputChanges)
    {
        getLogger().lifecycle("Checking idl and snapshot files for changes...");
        getLogger().info("idlFiles: " + _idlFiles.getAsPath());
        getLogger().info("snapshotFiles: " + _snapshotFiles.getAsPath());

        Set<String> filesRemoved = new HashSet<>();
        Set<String> filesAdded = new HashSet<>();
        Set<String> filesChanged = new HashSet<>();

        if (inputChanges.isIncremental())
        {
            Consumer<FileChange> handleChange = change ->
            {
                switch (change.getChangeType()) {
                    case ADDED:
                        filesAdded.add(change.getFile().getAbsolutePath());
                        break;
                    case REMOVED:
                        filesRemoved.add(change.getFile().getAbsolutePath());
                        break;
                    case MODIFIED:
                        filesChanged.add(change.getFile().getAbsolutePath());
                        break;
                }
            };

            inputChanges.getFileChanges(_idlFiles).forEach(handleChange);
            inputChanges.getFileChanges(_snapshotFiles).forEach(handleChange);

            if (!filesRemoved.isEmpty())
            {
                String files = joinByComma(filesRemoved);
                _needCheckinFiles.add(files);
                getLogger().lifecycle(
                        "The following files have been removed, be sure to remove them from source control: {}", files);
            }

            if (!filesAdded.isEmpty())
            {
                String files = joinByComma(filesAdded);
                _needCheckinFiles.add(files);
                getLogger().lifecycle("The following files have been added, be sure to add them to source control: {}", files);
            }

            if (!filesChanged.isEmpty())
            {
                String files = joinByComma(filesChanged);
                _needCheckinFiles.add(files);
                getLogger().lifecycle(
                        "The following files have been changed, be sure to commit the changes to source control: {}", files);
            }
        }
    }

    private String joinByComma(Set<String> files)
    {
        return files.stream().collect(Collectors.joining(", "));
    }

    @InputFiles
    @SkipWhenEmpty
    public FileCollection getSnapshotFiles()
    {
        return _snapshotFiles;
    }

    public void setSnapshotFiles(FileCollection snapshotFiles)
    {
        _snapshotFiles = snapshotFiles;
    }

    @InputFiles
    @SkipWhenEmpty
    public FileCollection getIdlFiles()
    {
        return _idlFiles;
    }

    public void setIdlFiles(FileCollection idlFiles)
    {
        _idlFiles = idlFiles;
    }

    @Internal
    public Collection<String> getNeedCheckinFiles()
    {
        return _needCheckinFiles;
    }
}