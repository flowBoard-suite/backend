package pl.flow.board.backend.service;

import org.springframework.stereotype.Service;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.common.util.concurrent.MoreExecutors;

import lombok.RequiredArgsConstructor;
import pl.flow.board.backend.model.User;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final Firestore firestore;

    public Mono<User> createUser(User user) {
        user.setCreatedAt(Timestamp.now());

        return Mono.create(sink -> {
            var future = firestore.collection("users").add(user);

            ApiFutures.addCallback(future, new ApiFutureCallback<DocumentReference>() {
                @Override
                public void onSuccess(DocumentReference result) {
                    user.setId(result.getId());
                    sink.success(user);
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }
}
