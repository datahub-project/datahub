import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;

import javax.annotation.Nonnull;

public class RemoteEntityDao {

    protected final Client _restliClient;

    public RestliRemoteWriterDAO(@Nonnull Client restliClient) {
        _restliClient = restliClient;
    }

    @Override
    public <URN extends Urn> void create(@Nonnull URN urn, @Nonnull RecordTemplate snapshot)
            throws IllegalArgumentException, RestliClientException {
        ModelUtils.validateSnapshotUrn(snapshot.getClass(), urn.getClass());

        final Request request = RequestBuilders.getBuilder(urn).createRequest(urn, snapshot);

        try {
            _restliClient.sendRequest(request).getResponse();
        } catch (RemoteInvocationException e) {
            throw new RestliClientException(e);
        }
    }
}