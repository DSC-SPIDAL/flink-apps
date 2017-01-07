package edu.iu.dsc.flink.kmeans;

/**
 * A simple two-dimensional centroid, basically a point with an ID.
 */
public class Centroid extends Point {
    public int id;

    public transient int mapId;

    public Centroid() {}

    public Centroid(int id, double x, double y) {
        super(x,y);
        this.id = id;
    }

    public Centroid(int id, int mapId, double x, double y) {
        super(x, y);
        this.id = id;
        this.mapId = mapId;
    }

    public Centroid(int id, Point p) {
        super(p.x, p.y);
        this.id = id;
    }

    @Override
    public String toString() {
        return id + " " + super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Centroid centroid = (Centroid) o;

        return id == centroid.id;

    }

    @Override
    public int hashCode() {
        return id;
    }
}
