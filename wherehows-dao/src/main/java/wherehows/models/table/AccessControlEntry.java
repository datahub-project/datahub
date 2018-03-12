package wherehows.models.table;

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@NoArgsConstructor
public class AccessControlEntry {

  private String principal;

  private List<String> accessType;

  private String businessJustification;

  private Long expiresAt;
}
