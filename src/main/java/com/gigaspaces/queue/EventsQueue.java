package com.gigaspaces.queue;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Barak Bar Orion
 * on 3/30/16.
 *
 * @since 11.0
 */
public class EventsQueue<T extends Event> implements BlockingQueue<T> {
    private final int NUMBER_OF_EVENT_TYPE = 256;

    private final int MAX_SIZE;

    private List[] dataArray = new LinkedList[NUMBER_OF_EVENT_TYPE];

    private List<T> fifoQueue = new LinkedList<>();

    public EventsQueue(int size) {
        MAX_SIZE = size;

        for (int i = 0; i < NUMBER_OF_EVENT_TYPE; i++) {
            dataArray[i] = new LinkedList<>();
        }
    }

    @Override
    public synchronized T get() throws InterruptedException {
        while (fifoQueue.isEmpty()){
            wait();
        }
        notifyAll();
        T event = fifoQueue.remove(0);
        System.out.println("get - i GOT - " + Thread.currentThread().getName() + "-" + event);
        return event;
    }

    @Override
    public synchronized void put(T t) throws InterruptedException {
        Event e = t;

        while(fifoQueue.size() >=  MAX_SIZE){
            System.out.println("put- i am waiting - " + Thread.currentThread().getName() + "-" + e);
            wait();
        }
        System.out.println("put - I AM FREE !!! - " + Thread.currentThread().getName() + "-" + e);
        switch (t.getOperation()){

            case CREATE:
                handleCreate(t);
                break;

            case UPDATE:
                handleUpdate(t);
                break;

            default:
                handleDelete(t);
                break;
        }
        notifyAll();

    }

    private void handleDelete(T t) {
        byte id = t.getId();
        dataArray[id].clear();
    }

    private void handleUpdate(T t) {
        byte id = t.getId();
        int size = dataArray[id].size();
        if(size == 0){
            dataArray[id].add(t);
        }
        else if(size == 1){

            if(((Event)dataArray[id].get(0)).getOperation().equals(Op.CREATE)){
                dataArray[id].add(t);
            }
            else { // has update on date queue
                Object removedEvent = dataArray[id].remove(0);
                fifoQueue.remove(removedEvent);

                dataArray[id].add(t);
            }
        }
        else { // size = 2
            Object removedEvent = dataArray[id].remove(1);
            fifoQueue.remove(removedEvent);

            dataArray[id].add(t);
        }
        fifoQueue.add(t);
    }

    private void handleCreate(T t) {
        byte id = t.getId();
        if(dataArray[id].size() > 0){
            fifoQueue.removeAll(dataArray[id]);
            dataArray[id].clear();
        }
        fifoQueue.add(t);
        dataArray[id].add(t);
    }

    public void printFifo(){
        System.out.println(fifoQueue);
    }
}
