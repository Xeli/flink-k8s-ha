package eu.xeli.flink.ha;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;
import static org.mockito.Mockito.*;

public class FsCheckpointIDCounterTest {
    private FsCheckpointIDCounter checkpointIDCounter;

    private FileSystem fileSystem = mock(FileSystem.class);
    private Path checkpointCounterPath = mock(Path.class);
    private FSDataInputStream dataInputStream = mock(FSDataInputStream.class);
    private FSDataOutputStream dataOutputStream = mock(FSDataOutputStream.class);

    @Before
    public void setup() throws Exception {
        when(fileSystem.create(checkpointCounterPath, NO_OVERWRITE)).thenReturn(dataOutputStream);
        when(fileSystem.open(checkpointCounterPath)).thenReturn(dataInputStream);
        checkpointIDCounter = new FsCheckpointIDCounter(fileSystem, checkpointCounterPath);
    }
    @Test
    public void testStart() throws Exception {
        checkpointIDCounter.start();
        verify(fileSystem, times(1)).create(checkpointCounterPath, NO_OVERWRITE);
    }

    @Test
    public void testShutdown() throws Exception {
        checkpointIDCounter.start();
        checkpointIDCounter.shutdown(JobStatus.RECONCILING);

        verify(dataInputStream, times(1)).close();
        verify(dataOutputStream, times(1)).close();
    }

    @Test
    public void testShutdownGloballyTerminated() throws Exception {
        checkpointIDCounter.start();
        checkpointIDCounter.shutdown(JobStatus.FINISHED);

        verify(fileSystem, times(1)).delete(checkpointCounterPath, false);
    }
}