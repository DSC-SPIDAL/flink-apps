package edu.iu.dsc.flink.kmeans;

import java.util.List;

public class PointBlock {
    public List<Point> points;

    public PointBlock(List<Point> points) {
        this.points = points;
    }

    public PointBlock() {
    }
}
