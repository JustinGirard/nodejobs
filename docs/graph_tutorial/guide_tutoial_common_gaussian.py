"""
guide_tutoial_common_gaussian.py

Shared, tutorial-grade utilities for the graph optimization series.

This file exists so Tutorial 03 and Tutorial 04 do not need to repeat:

- BaseData payload types (Point, BasicModel, Gaussian2DParams)
- The mature 2D Gaussian (ellipse + rotation) sampling math

All tutorials in this series treat `seed` as optional:

- if seed is None, we derive a time-based seed_used so repeat runs produce new results
- we return/record seed_used so runs are reproducible when needed
"""

import math
import random
import time

from nodejobs.dependencies.BaseData import BaseData


class Point(BaseData):
    x: float
    y: (float, None)


class BasicModel(BaseData):
    m: float
    b: float


class Gaussian2DParams(BaseData):
    mu_x: float
    mu_y: float
    std_major: float
    std_minor: float
    theta_deg: float


def sample_gaussian2d_points(
    *,
    n: int,
    gauss: Gaussian2DParams,
    seed: int | None = None,
) -> tuple[list[Point], int]:
    """Sample a rotated-ellipse 2D Gaussian.

    Parameterization:
      - translation: mu_x, mu_y
      - ellipse spread: std_major, std_minor
      - rotation: theta_deg

    Returns:
      (points, seed_used)
    """
    seed_used = int(seed) if seed is not None else int(time.time_ns() % (2**31 - 1))
    rng = random.Random(seed_used)

    gauss = Gaussian2DParams(gauss)
    mu_x = float(gauss.mu_x)
    mu_y = float(gauss.mu_y)
    std_major = float(gauss.std_major)
    std_minor = float(gauss.std_minor)
    theta = math.radians(float(gauss.theta_deg))

    if std_major < 0.0 or std_minor < 0.0:
        raise ValueError("std_major and std_minor must be >= 0")

    c = math.cos(theta)
    s = math.sin(theta)

    points: list[Point] = []
    for _ in range(int(n)):
        # u,v are independent standard normals
        u = float(rng.gauss(0.0, 1.0))
        v = float(rng.gauss(0.0, 1.0))

        # Scale to ellipse
        a = std_major * u
        b = std_minor * v

        # Rotate + translate
        x = mu_x + (c * a) - (s * b)
        y = mu_y + (s * a) + (c * b)
        points.append(Point({Point.x: float(x), Point.y: float(y)}))

    return points, seed_used
