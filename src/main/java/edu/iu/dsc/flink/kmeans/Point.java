package edu.iu.dsc.flink.kmeans;

import java.io.Serializable;

/**
 * A simple two-dimensional point.
 */
public class Point implements Serializable {

    public double x, y;

    public double[]values;

    public Point() {}

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point(double[] values) {
        this.values = values;
    }

    public Point add(Point other) {
        x += other.x;
        y += other.y;
        if (other.values != null) {
            for (int i = 0; i < values.length; i++) {
                values[i] = other.values[i];
            }
        }
        return this;
    }

    public Point div(long val) {
        x /= val;
        y /= val;
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                values[i] = values[i] / val;
            }
        }
        return this;
    }

    public double euclideanDistance(Point other) {
        if (values == null) {
            return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
        } else {
            double sum = 0;
            for (int i = 0; i < values.length; i++) {
                sum += (values[i] - other.values[i]) * (values[i] - other.values[i]);
            }
            return Math.sqrt(sum);
        }
    }

    public void clear() {
        x = y = 0.0;
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                values[i] = 0;
            }
        }
    }

    @Override
    public String toString() {
        if (values == null) {
            return x + " " + y;
        } else {
            String s = "";
            for (int i = 0; i < values.length; i++) {
                s += values[i] + " ";
            }
            return s;
        }
    }
}
