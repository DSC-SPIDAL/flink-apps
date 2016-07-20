package edu.iu.dsc.flink.kmeans;

import java.io.Serializable;

/**
 * A simple two-dimensional point.
 */
public class Point implements Serializable {

    public double x, y;

    public long time;

    public Point() {}

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point(double x, double y, long time) {
        this.x = x;
        this.y = y;
        this.time = time;
    }

    public Point add(Point other) {
        x += other.x;
        y += other.y;
        return this;
    }

    public Point div(long val) {
        x /= val;
        y /= val;
        return this;
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y));
    }

    public void clear() {
        x = y = 0.0;
    }

    @Override
    public String toString() {
        return x + " " + y + " " + (((double)time) / 1000000);
    }
}
