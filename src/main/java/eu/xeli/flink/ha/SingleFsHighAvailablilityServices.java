package eu.xeli.flink.ha;

import java.io.IOException;
import java.util.concurrent.Executor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.FileSystemBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.dispatcher.SingleJobSubmittedJobGraphStore;
import org.apache.flink.runtime.highavailability.FsNegativeRunningJobsRegistry;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.nonha.leaderelection.SingleLeaderElectionService;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;

/**
 * A {@link HighAvailabilityServices} implementation
 */
public class SingleFsHighAvailablilityServices implements HighAvailabilityServices {
    private static final String BLOB_STORE = "blobstore";
    private static final String RUNNING_JOBS_REGISTRY = "runningJobsRegistry";
    private static final String CHECKPOINT_RECOVERY = "checkpointRecovery";

    private final Path root;
    private final LeaderRetrievalService leaderRetrievalService;
    private final LeaderElectionService leaderElectionService;

    SingleFsHighAvailablilityServices(Executor executor,
                                      Path root,
                                      String jobManagerAddress) {
        this.root = root;
        this.leaderRetrievalService = new SettableLeaderRetrievalService(jobManagerAddress, DEFAULT_LEADER_ID);
        this.leaderElectionService = new SingleLeaderElectionService(executor, DEFAULT_LEADER_ID);
    }

    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        return leaderRetrievalService;
    }

    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        return leaderRetrievalService;
    }

    public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
        return leaderRetrievalService;
    }

    public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String s) {
        return leaderRetrievalService;
    }

    public LeaderRetrievalService getWebMonitorLeaderRetriever() {
        return leaderRetrievalService;
    }

    public LeaderElectionService getResourceManagerLeaderElectionService() {
        return leaderElectionService;
    }

    public LeaderElectionService getDispatcherLeaderElectionService() {
        return leaderElectionService;
    }

    public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
        return leaderElectionService;
    }

    public LeaderElectionService getWebMonitorLeaderElectionService() {
        return leaderElectionService;
    }

    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
        return new FsCheckpointRecoveryFactory(createFilesystem(), new Path(root, CHECKPOINT_RECOVERY));
    }

    public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
        return new SingleJobSubmittedJobGraphStore(new JobGraph(DEFAULT_JOB_ID, null));
    }

    public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
        return new FsNegativeRunningJobsRegistry(createFilesystem(), new Path(root, RUNNING_JOBS_REGISTRY));
    }

    public BlobStore createBlobStore() throws IOException {
        return new FileSystemBlobStore(createFilesystem(), new Path(root, BLOB_STORE).getPath());
    }

    public void close() throws Exception {
    }

    public void closeAndCleanupAllData() throws Exception {
        createFilesystem().delete(root, true);
    }

    private FileSystem createFilesystem() {
        try {
            return root.getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
