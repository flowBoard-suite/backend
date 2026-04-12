package pl.flow.board.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pl.flow.board.backend.model.Activity;
import pl.flow.board.backend.service.ActivityService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/boards/{boardId}/activities")
@RequiredArgsConstructor
public class ActivityController {

    private final ActivityService activityService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Activity> createActivity(@PathVariable String boardId, @RequestBody Activity activity) {
        activity.setBoardId(boardId);
        return activityService.saveActivity(activity);
    }

    @GetMapping
    public Flux<Activity> getAllActivitiesForBoard(@PathVariable String boardId) {
        return activityService.getAllActivitiesForBoard(boardId);
    }

    @GetMapping("/{activityId}")
    public Mono<Activity> getActivityById(@PathVariable String activityId) {
        return activityService.getActivityById(activityId);
    }

    @PutMapping("/{activityId}")
    public Mono<Activity> updateActivity(@PathVariable String activityId, @RequestBody Activity activity) {
        return activityService.updateActivity(activityId, activity);
    }

    @DeleteMapping("/{activityId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteActivity(@PathVariable String activityId) {
        return activityService.deleteActivity(activityId);
    }
}