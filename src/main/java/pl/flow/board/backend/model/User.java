package pl.flow.board.backend.model;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.annotation.DocumentId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {

    @DocumentId
    private String id;

    private String email;
    private String username;
    private String password;

    private Timestamp createdAt;

    private String boardId;
}
