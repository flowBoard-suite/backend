package pl.flow.board.backend.repository;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import pl.flow.board.backend.exception.custom.ResourceNotFoundException;
import pl.flow.board.backend.interfaces.UserRepository;
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
            ApiFuture<DocumentReference> future = firestore.collection("users").add(user);

            return FirestoreUtils.toMono(future)
                    .map(docRef -> {
                        user.setId(docRef.getId());
                        return user;
                    });
        }

        ApiFuture<WriteResult> future = firestore.collection("users")
                .document(user.getId()).set(user);

        return FirestoreUtils.toMono(future)
                .map(writeResult -> user);
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
