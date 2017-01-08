package edu.iu.dsc.flink.kmeans;

import java.util.List;

public class PointBlock {
    public List<Point2> points;

    public PointBlock(List<Point2> points) {
        this.points = points;
    }

    public PointBlock() {
    }
}
