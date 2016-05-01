package com.gigaspaces.queue;

/**
 * Created by tamirs
 * on 3/31/16.
 */
public class TestQueue {

    public static EventsQueue eventsQueue = new EventsQueue(5);

    public static void main(String[] args) throws InterruptedException {

        for(int i = 0; i < 10; i++){
            new Thread(new Creater((byte) i)).start();
        }

        new Thread(new Getter()).start();

    }

    public static class Creater implements Runnable{
        private byte id;
        public Creater(byte id) {
            this.id=id;
        }

        @Override
        public void run() {
            while(true){
                try {
                    eventsQueue.put(new Event(id, Op.CREATE));
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public static class Getter implements Runnable{
        public Getter() {
        }

        @Override
        public void run() {
            while(true){
                try {
                    Event event = eventsQueue.get();
                    Thread.sleep(10000);
                    System.out.println("just got " + event);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

}
