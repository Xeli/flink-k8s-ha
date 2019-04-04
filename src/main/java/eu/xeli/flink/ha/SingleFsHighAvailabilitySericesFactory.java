package eu.xeli.flink.ha;

import java.util.concurrent.Executor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;

import static org.apache.flink.configuration.HighAvailabilityOptions.HA_STORAGE_PATH;
import static org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.getJobManagerAddress;

public class SingleFsHighAvailabilitySericesFactory implements HighAvailabilityServicesFactory {
    @Override
    public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) throws Exception {
        return new SingleFsHighAvailablilityServices(executor,
                                                     new Path(configuration.getString(HA_STORAGE_PATH)),
                                                     getJobManagerAddress(configuration).f0);
    }
}
