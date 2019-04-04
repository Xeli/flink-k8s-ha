package eu.xeli.flink.ha;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.InstantiationUtil;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

class FsCompletedCheckpointStore implements CompletedCheckpointStore {
    private final FileSystem fileSystem;
    private final Path checkpointStore;
    private final int maxNumberOfRetainedCheckpoints;

    private ArrayDeque<CompletedCheckpoint> completedCheckpoints;

    FsCompletedCheckpointStore(FileSystem fileSystem, Path checkpointStore, int maxNumberOfRetainedCheckpoints) {
        this.fileSystem = fileSystem;
        this.checkpointStore =  checkpointStore;
        this.maxNumberOfRetainedCheckpoints = maxNumberOfRetainedCheckpoints;
    }

    @Override
    public void recover() throws Exception {
        completedCheckpoints = new ArrayDeque<>();

        if (fileSystem.exists(checkpointStore)) {
            fileSystem.mkdirs(checkpointStore);
            return;
        }

        //retrieve checkpoints from files in  checkpointStore directory
        for (FileStatus fileStatus : fileSystem.listStatus(checkpointStore)) {
            if (fileStatus.isDir()) {
                throw new RuntimeException("Found unexpected directory in checkpoint store: " + checkpointStore.getPath());
            }
            completedCheckpoints.add(completedCheckpointFromFile(fileStatus.getPath()));
        }
    }

    private CompletedCheckpoint completedCheckpointFromFile(Path path) throws Exception {
        try (FSDataInputStream inputStream = fileSystem.open(path)) {
            return InstantiationUtil.deserializeObject(inputStream, Thread.currentThread().getContextClassLoader());
        }
    }

    @Override
    public void addCheckpoint(CompletedCheckpoint completedCheckpoint) throws Exception {
        try (FSDataOutputStream outputStream = fileSystem.create(getPath(completedCheckpoint), NO_OVERWRITE)) {
            InstantiationUtil.serializeObject(outputStream, completedCheckpoint);
            completedCheckpoints.add(completedCheckpoint);
        }
    }

    @Override
    public CompletedCheckpoint getLatestCheckpoint() throws Exception {
        if (completedCheckpoints.isEmpty()) {
            return null;
        }
        return completedCheckpoints.peekFirst();
    }

    @Override
    public void shutdown(JobStatus jobStatus) throws Exception {
        if(jobStatus.isGloballyTerminalState()) {
            fileSystem.delete(checkpointStore, false);
        }
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
        return new ArrayList<>(completedCheckpoints);
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return completedCheckpoints.size();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return maxNumberOfRetainedCheckpoints;
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return true;
    }

    private Path getPath(CompletedCheckpoint completedCheckpoint) {
        return new Path(checkpointStore, String.valueOf(completedCheckpoint.getCheckpointID()));
    }
}
