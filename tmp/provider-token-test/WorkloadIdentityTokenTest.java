import java.util.ServiceLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

public class WorkloadIdentityTokenTest {

    public static void main(String[] args) throws Exception {

        for (CustomTokenProviderAdaptee provider :
                ServiceLoader.load(CustomTokenProviderAdaptee.class)) {

            System.out.println("PROVIDER: " +
                    provider.getClass().getName());

            provider.initialize(
                    new Configuration(),
                    "flinkstorage911.dfs.core.windows.net"
            );

            String token = provider.getAccessToken();

            if (token == null || token.isEmpty()) {
                throw new RuntimeException("Access token is empty");
            }

            System.out.println("ACCESS_TOKEN_OK");
            System.out.println("TOKEN_LENGTH: " + token.length());
            return;
        }

        throw new RuntimeException("Custom provider not found");
    }
}
