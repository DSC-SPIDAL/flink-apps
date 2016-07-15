package edu.iu.dsc.flink.kmeans;

import edu.iu.dsc.flink.kmeans.utils.Timing;

import java.io.Serializable;

/**
 * A simple two-dimensional point.
 */
public class Point implements Serializable {

    public double x, y;

    public Point() {}

    public int pid;

    public long time;

    public Point(double x, double y, int id, long time) {
        this.x = x;
        this.y = y;
        this.pid = id;
        this.time = time;
    }

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
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
        return x + " " + y;
    }
}
