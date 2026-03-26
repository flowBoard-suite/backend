package pl.flow.board.backend.model;

import com.google.cloud.firestore.annotation.DocumentId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Board {

    @DocumentId
    private String id;

    private String name;
    private String serialNumber;
    private String description;
}
