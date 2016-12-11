package rx.observables;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

import rx.Subscriber;
import rx.internal.util.BackpressureDrainManager;

class SplitStringSubscriber extends Subscriber<String> implements BackpressureDrainManager.BackpressureQueueCallback {

    private int emptyPartCount = 0;
    private String leftOver = null;
    private final Pattern pattern;
    private Subscriber<? super String> child;
    private final BackpressureDrainManager manager;
    private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();

    public SplitStringSubscriber(Subscriber<? super String> child, final Pattern pattern) {
        this.pattern = pattern;
        this.child = child;
        this.manager = new BackpressureDrainManager(this);
    }

    public BackpressureDrainManager getManager() {
        return manager;
    }

    @Override
    public void onStart() {
        request(Long.MAX_VALUE);
    }

    @Override
    public void onCompleted() {
        if (leftOver != null)
            output(leftOver);
        manager.terminateAndDrain();
    }

    @Override
    public void onError(Throwable e) {
        if (leftOver != null)
            output(leftOver);
        manager.terminateAndDrain(e);
    }

    @Override
    public void onNext(String segment) {
        if (leftOver != null)
            segment = leftOver + segment;
        String[] parts = pattern.split(segment, -1);        
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            output(part);
        }
        String last = parts[parts.length - 1];
        leftOver = last;
        if (last != null && !last.isEmpty()) {
            // we have leftOver so we must request at least one part
            this.request(1);
        }
    }

    /**
     * when limit == 0 trailing empty parts are not emitted.
     * 
     * @param part
     */
    private void output(String part) {
        if (part.isEmpty()) {
            emptyPartCount++;
        } else {
            for (; emptyPartCount > 0; emptyPartCount--)
                if (!child.isUnsubscribed())
                    addNext("");
            if (!child.isUnsubscribed())
                addNext(part);
        }
    }

    private void addNext(String part) {
        queue.offer(part);
        manager.drain();
    }

    @Override
    public Object peek() {
        return queue.peek();
    }

    @Override
    public Object poll() {
        return queue.poll();
    }

    @Override
    public boolean accept(Object value) {
        child.onNext((String) value);
        return false;
    }

    @Override
    public void complete(Throwable exception) {
        if (exception != null) {
            child.onError(exception);
        } else {
            child.onCompleted();
        }
    }

}
