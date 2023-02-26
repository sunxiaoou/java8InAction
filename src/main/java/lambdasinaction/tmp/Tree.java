package lambdasinaction.tmp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Tree implements Serializable {
    static class Node {
        String name;
        Object data;
        List<Node> children;

        Node(String name, Object data) {
            this.name = name;
            this.data = data;
            this.children = new ArrayList<>();
        }
    }

    Node root;

    Tree(String name, Object data) {
        root = new Node(name, data);
    }

    public Node search(Node node, String name) {
        if (node == null) {
            node = root;
        }
        if (node.name.equals(name)) {
            return node;
        }
        for (Node child: node.children) {
            node = search(child, name);
            if (node != null)
                return node;
        }
        return null;
    }

    public boolean addChild(String parent, String name, Object data) {
        Node node = search(null, parent);
        if (node == null) {
            return false;
        }
        node.children.add(new Node(name, data));
        return true;
    }

    // Preorder traversal
    public void preOrder(Node node, StringBuilder sb) {
        if (node == null) {
            return;
        }
        // System.out.println(node.name + " " + node.data);
        sb.append(node.name).append(" ").append(node.data).append("\n");
        for (Node child : node.children) {
            preOrder(child, sb);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        preOrder(root, sb);
        return sb.toString();
    }

    public static void main(String[] args) {
        Tree tree = new Tree("root", null);
        tree.addChild("root", "node01", null);
        tree.addChild("root", "node02", null);
        tree.addChild("root", "node03", null);
        tree.addChild("node02", "node021", null);
        System.out.println(tree.addChild("unknown", "unknown", null));
        System.out.println(tree);
    }
}
