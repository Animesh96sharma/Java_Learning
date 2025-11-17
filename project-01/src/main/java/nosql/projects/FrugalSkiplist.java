package nosql.projects;

import java.util.ArrayList;
import java.util.List;

import nosql.projects.Material.VersionList;

public final class FrugalSkiplist<P> implements VersionList <P> {
    private static final class Node<P> {
        final long ts;
        final P payload;
        final Node<P> nextNode;
        final Node<P> vRidgy;
        
        Node(long ts, P payload, Node<P> nextNode, Node<P> vRidgy) {
            this.ts = ts;
            this.payload = payload;
            this.nextNode = nextNode;
            this.vRidgy = vRidgy;
        }
    }
    
    private Node<P> head;
    private long count = 0;
    private final List<Node<P>> lastAtLevel = new ArrayList<>();

    @Override
    public void append(P point, long timeStamp) {
        count++;
        int level = Long.numberOfTrailingZeros(count);
        
        while (lastAtLevel.size() <= level) {
            lastAtLevel.add(null);
        }

        Node<P> ridgyTarget = lastAtLevel.get(level);
        Node<P> newHead = new Node<>(timeStamp, point, head, ridgyTarget);

        head = newHead;
        lastAtLevel.set(level, newHead);
    }

    @Override
    public P findVisible(long t) {
        Node<P> current = head;
        while (current != null && current.ts > t) {
            if (current.vRidgy != null && current.vRidgy.ts > t) {
                current = current.vRidgy;
            } else {
                current = current.nextNode;
            }
        }
        return (current != null) ? current.payload : null;
    }
}
