package pl.flow.board.backend.model;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.annotation.DocumentId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Activity {

    @DocumentId
    private String id;
    private String boardId;
    private String title;
    private String description;
    private String status;
    private Timestamp date;

}
