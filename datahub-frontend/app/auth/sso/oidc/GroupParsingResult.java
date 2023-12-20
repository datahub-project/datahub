package auth.sso.oidc;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import java.util.List;
public class GroupParsingResult {
    private List<CorpGroupSnapshot> corpGroupSnapshots;
    private String errorMessage;

    public GroupParsingResult(List<CorpGroupSnapshot> corpGroupSnapshots, String errorMessage) {
        this.corpGroupSnapshots = corpGroupSnapshots;
        this.errorMessage = errorMessage;
    }

    public List<CorpGroupSnapshot> getCorpGroupSnapshots() {
        return corpGroupSnapshots;
    }

    public void setCorpGroupSnapshots(List<CorpGroupSnapshot> corpGroupSnapshots) {
        this.corpGroupSnapshots = corpGroupSnapshots;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public boolean hasError() {
        return errorMessage != null && !errorMessage.isEmpty();
    }
}
