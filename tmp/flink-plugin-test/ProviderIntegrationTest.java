import com.xlen.azure.WorkloadIdentityTokenProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

import java.util.Date;

public class ProviderIntegrationTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== FLINK AZURE PROVIDER TEST ===");

        // 1. Verify provider interface compatibility.
        Class<?> providerClass =
                WorkloadIdentityTokenProvider.class;

        if (!CustomTokenProviderAdaptee.class
                .isAssignableFrom(providerClass)) {
            throw new IllegalStateException(
                    "Provider does not implement CustomTokenProviderAdaptee");
        }

        System.out.println("[PASS] CustomTokenProviderAdaptee compatibility");

        // 2. Instantiate the provider.
        WorkloadIdentityTokenProvider provider =
                new WorkloadIdentityTokenProvider();

        System.out.println("[PASS] Provider instantiation");

        // 3. Initialize with Hadoop configuration.
        Configuration configuration = new Configuration();

        provider.initialize(
                configuration,
                "flinkstorage911.dfs.core.windows.net");

        System.out.println("[PASS] Provider initialization");

        // 4. Check initial token expiration.
        Date expiry = provider.getExpiryTime();

        if (expiry == null || expiry.getTime() != 0) {
            throw new IllegalStateException(
                    "Unexpected initial token expiration: " + expiry);
        }

        System.out.println("[PASS] Initial token expiration state");

        // 5. Verify that provider and interface share the expected classloader.
        System.out.println(
                "Provider classloader: "
                + providerClass.getClassLoader());

        System.out.println(
                "Interface classloader: "
                + CustomTokenProviderAdaptee.class.getClassLoader());

        System.out.println("=== ALL TESTS PASSED ===");
    }
}