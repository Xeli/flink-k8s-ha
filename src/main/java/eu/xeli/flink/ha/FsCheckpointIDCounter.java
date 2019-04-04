package eu.xeli.flink.ha;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

public class FsCheckpointIDCounter implements CheckpointIDCounter {
    private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointIDCounter.class);

    private final FileSystem fileSystem;
    private final Path checkpointFilePath;

    FsCheckpointIDCounter(FileSystem fileSystem, Path checkpointFilePath) {
        this.fileSystem = fileSystem;
        this.checkpointFilePath = checkpointFilePath;
    }

    public void start() throws Exception {
    }

    public void shutdown(JobStatus jobStatus) throws Exception {
        if(jobStatus.isGloballyTerminalState()) {
            fileSystem.delete(checkpointFilePath, false);
        }
    }

    public long getAndIncrement() throws Exception {
        try (DataInputStream inputStream = new DataInputStream(fileSystem.open(checkpointFilePath))) {
            long currentValue = inputStream.readLong();
            try (DataOutputStream outputStream = new DataOutputStream(fileSystem.create(checkpointFilePath, NO_OVERWRITE))) {
                outputStream.writeLong(currentValue + 1);
                return currentValue;
            }
        }
    }

    public void setCount(long value) throws Exception {
        try (DataOutputStream outputStream = new DataOutputStream(fileSystem.create(checkpointFilePath, NO_OVERWRITE))) {
            outputStream.writeLong(value);
        }
    }
}
