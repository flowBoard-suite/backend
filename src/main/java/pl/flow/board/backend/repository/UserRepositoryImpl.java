package pl.flow.board.backend.repository;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import pl.flow.board.backend.exception.custom.ResourceNotFoundException;
import pl.flow.board.backend.model.User;
import pl.flow.board.backend.utils.FirestoreUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Repository
@RequiredArgsConstructor
public class UserRepositoryImpl implements UserRepository {

    private final Firestore firestore;

    @Override
    public Mono<User> save(User user) {
        if (user.getId() == null || user.getId().isBlank()) {
            return Mono.create(sink -> {
                ApiFuture<DocumentReference> future = firestore.collection("users").add(user);
                ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
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

        return Mono.create(sink -> {
            ApiFuture<com.google.cloud.firestore.WriteResult> future = firestore.collection("users")
                    .document(user.getId()).set(user);
            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(com.google.cloud.firestore.WriteResult result) {
                    sink.success(user);
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    @Override
    public Mono<User> findById(String id) {
        ApiFuture<DocumentSnapshot> future = firestore.collection("users").document(id).get();

        return FirestoreUtils.toMono(future).flatMap(snapshot -> {
            if (!snapshot.exists()) {
                return Mono.error(new ResourceNotFoundException("User not found"));
            }

            return Mono.justOrEmpty(snapshot.toObject(User.class));

        })
                .log("UserRepositoryImpl.findById")
                .subscribeOn(Schedulers.boundedElastic());
    }
}
