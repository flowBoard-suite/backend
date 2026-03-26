package pl.flow.board.backend.service;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pl.flow.board.backend.model.Board;
import pl.flow.board.backend.model.User;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserService {

    private final Firestore firestore;

    private final BoardService boardService;

    public Mono<User> getUserById(String userId) {
        return Mono.create(sink -> {
            var docRef = firestore.collection("users").document(userId).get();

            ApiFutures.addCallback(docRef, new ApiFutureCallback<DocumentSnapshot>() {
                @Override
                public void onSuccess(DocumentSnapshot result) {
                    if (result.exists()) {
                        User user = result.toObject(User.class);
                        if (user != null) {
                            user.setId(result.getId());
                        }
                        sink.success(user);
                    } else {
                        sink.error(new RuntimeException("Użytkownik o ID " + userId + " nie istnieje"));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    public Mono<User> assignBoardToUser(String userId, Board board) {
        return boardService.saveBoard(board)
                .flatMap(savedBoard -> {
                    return Mono.<Void>create(sink -> {
                        var updateFuture = firestore.collection("users").document(userId)
                                .update("boardId", savedBoard.getId());

                        ApiFutures.addCallback(updateFuture, new com.google.api.core.ApiFutureCallback<>() {
                            @Override
                            public void onSuccess(com.google.cloud.firestore.WriteResult result) {
                                sink.success();
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                sink.error(t);
                            }
                        }, MoreExecutors.directExecutor());
                    }).then(getUserById(userId));
                });
    }
}
