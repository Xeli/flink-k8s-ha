package eu.xeli.flink.ha;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;

import static java.lang.String.format;

public class FsCheckpointRecoveryFactory implements CheckpointRecoveryFactory {
    private static final String CHECKPOINT_COUNTER_FILENAME = "checkpointIDCounter";
    private static final String CHECKPOINT_STORE_DIRNAME = "checkpointStore";
    private final FileSystem fileSystem;
    private final Path checkpointRecoveryPath;

    public FsCheckpointRecoveryFactory(FileSystem fileSystem, Path checkpointRecoveryPath) {
        this.fileSystem = fileSystem;
        this.checkpointRecoveryPath = checkpointRecoveryPath;
    }

    @Override
    public CompletedCheckpointStore createCheckpointStore(JobID jobID, int maxNumberOfRetainedCheckpoints, ClassLoader classLoader) throws Exception {
        return new FsCompletedCheckpointStore(fileSystem, pathWithSuffix(CHECKPOINT_STORE_DIRNAME), maxNumberOfRetainedCheckpoints);
    }

    @Override
    public CheckpointIDCounter createCheckpointIDCounter(JobID jobID) throws Exception {
        return new FsCheckpointIDCounter(fileSystem, pathWithSuffix(CHECKPOINT_COUNTER_FILENAME));
    }

    private Path pathWithSuffix(String name) {
        return new Path(format("%s%s%s", checkpointRecoveryPath, Path.SEPARATOR_CHAR, name));
    }
}
