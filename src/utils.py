import numpy as np

def quaternion_to_rotation_matrix(q):
    """
    Convert quaternion [w, x, y, z] or [x, y, z, w] to rotation matrix.
    This function attempts to detect the order based on context or assumes [x, y, z, w] if not specified,
    but standard convention varies.
    
    Here we assume input q is [w, x, y, z] which is common in datasets like NuScenes.
    """
    w, x, y, z = q
    R = np.array([
        [1 - 2*y*y - 2*z*z,     2*x*y - 2*z*w,     2*x*z + 2*y*w],
        [2*x*y + 2*z*w,     1 - 2*x*x - 2*z*z,     2*y*z - 2*x*w],
        [2*x*z - 2*y*w,         2*y*z + 2*x*w,     1 - 2*x*x - 2*y*y]
    ])
    return R

def transform_points(points, rotation, translation):
    """
    Apply rigid body transformation to points.
    points: (N, 3)
    rotation: (4,) quaternion [x, y, z, w] or rotation matrix (3, 3)
    translation: (3,) [x, y, z]
    """
    if len(rotation) == 4:
        R = quaternion_to_rotation_matrix(rotation)
    else:
        R = np.array(rotation)
    
    T = np.array(translation)
    
    # P_new = R @ P_old + T
    # For N points: (N, 3) @ R.T + T
    return points @ R.T + T
