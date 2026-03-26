package pl.flow.board.backend.service;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pl.flow.board.backend.model.Board;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class BoardService {

    private final Firestore firestore;

    public Mono<Board> saveBoard(Board board) {
        return Mono.create(sink -> {
            ApiFuture<DocumentReference> future = firestore.collection("boards").add(board);

            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(DocumentReference result) {
                    board.setId(result.getId());
                    sink.success(board);
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }
}