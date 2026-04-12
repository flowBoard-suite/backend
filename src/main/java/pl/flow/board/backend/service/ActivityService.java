package pl.flow.board.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import pl.flow.board.backend.model.Activity;
import pl.flow.board.backend.repository.ActivityRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ActivityService {

    private final ActivityRepository activityRepository;

    public Mono<Activity> saveActivity(Activity activity) {
        return activityRepository.save(activity);
    }

    public Flux<Activity> getAllActivitiesForBoard(String boardId) {
        return activityRepository.findAllByBoardId(boardId);
    }

    public Mono<Activity> getActivityById(String activityId) {
        return activityRepository.findById(activityId);
    }

    public Mono<Void> deleteActivity(String activityId) {
        return activityRepository.deleteById(activityId);
    }

    public Mono<Activity> updateActivity(String activityId, Activity activity) {
        activity.setId(activityId);
        return activityRepository.save(activity);
    }
}