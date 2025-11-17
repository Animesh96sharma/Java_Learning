package nosql.projects;

import nosql.projects.Material.VersionList;

public final class VLinkedList<P> implements VersionList<P> {
    private static final class Node<P> {
        final long ts;
        final P payload;
        Node<P> nextNode;
        Node(long ts, P payload, Node<P> nextNode) {
            this.ts = ts;
            this.payload = payload;
            this.nextNode = nextNode;
        }        
    }

    private Node<P> head;

    @Override
    public void append(P point, long timeStamp) {
        Node<P> newNode = new Node<>(timeStamp, point, null);

        if(head ==null || timeStamp >= head.ts) {
            newNode.nextNode = head;
            head = newNode;
            return;
        }

        Node<P> prevNode = head;
        Node<P> currentNode = head.nextNode;

        while (currentNode != null && currentNode.ts > timeStamp) {
            prevNode = currentNode;
            currentNode = currentNode.nextNode;
        }

        newNode.nextNode = currentNode;
        prevNode.nextNode = newNode;
    }

    @Override
    public P findVisible(long timeStamp) {
        Node<P> currentNode = head;
        while (currentNode != null) {
            if (currentNode.ts <= timeStamp)
                return currentNode.payload;
            currentNode = currentNode.nextNode;
        }
        return null;
    }
}
