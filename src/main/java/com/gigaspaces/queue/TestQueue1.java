package com.gigaspaces.queue;

/**
 * Created by tamirs
 * on 3/31/16.
 */
public class TestQueue1 {

    private static EventsQueue eventsQueue = new EventsQueue(11);

    public static void main(String[] args) throws InterruptedException {


        for (int i = 0; i < 5; i++) {
            eventsQueue.put(new Event((byte)i,Op.CREATE));
        }

        for (int i = 0; i < 5; i++) {
            eventsQueue.put(new Event((byte)i,Op.UPDATE));
        }
//        eventsQueue.printFifo();
//        new Thread(new Getter()).start();

        for (int i = 0; i < 5; i++) {
            eventsQueue.put(new Event((byte)i,Op.UPDATE));
        }
        eventsQueue.printFifo();

    }

    public static class Creater implements Runnable{
        private byte id;
        public Creater(byte id) {
            this.id=id;
        }

        @Override
        public void run() {
            try {
                eventsQueue.put(new Event(id, Op.CREATE));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            while(true){
                try {
                    eventsQueue.put(new Event(id, Op.UPDATE));
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
                    Thread.sleep(2000);
                    Event event = eventsQueue.get();
                    Thread.sleep(2000);
                    System.out.println("just got " + event);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }



}

