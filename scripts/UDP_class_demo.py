import time
import numpy as np
import pyrealsense2 as rs
from LocalNode.udp_video_comms_node import UDPVideoCommsNode


def depth_to_pointcloud(depth_frame, color_frame, depth_scale, depth_intrinsics, color_intrinsics, extrinsics, clip_distance_max=3.5, edge_margin=5, decimation_factor=1):
    """
    Convert RealSense depth and color frames to point cloud with manual reprojection

    Args:
        depth_frame: RealSense depth frame (primary)
        color_frame: RealSense color frame (separate, no alignment)
        depth_scale: Depth scale factor from camera
        depth_intrinsics: Depth camera intrinsics
        color_intrinsics: Color camera intrinsics
        extrinsics: Transformation from depth to color camera
        clip_distance_max: Maximum distance in meters
        edge_margin: Margin from image edges to exclude (pixels)
        decimation_factor: Only process every Nth pixel (1=all pixels, 2=every other pixel)

    Returns:
        numpy array of shape (N, 6) containing [x, y, z, r, g, b] for each point
    """

    # Convert to numpy arrays
    depth_image = np.asanyarray(depth_frame.get_data()) * depth_scale  # Convert to meters
    color_image = np.asanyarray(color_frame.get_data())

    rows, cols = depth_image.shape

    # Apply decimation - only process every Nth pixel
    if decimation_factor > 1:
        depth_image = depth_image[::decimation_factor, ::decimation_factor]
        rows, cols = depth_image.shape

    # Create coordinate grids
    c, r = np.meshgrid(np.arange(cols), np.arange(rows), sparse=True)
    r = r.astype(float)
    c = c.astype(float)

    # Adjust coordinates if decimated
    if decimation_factor > 1:
        c = c * decimation_factor
        r = r * decimation_factor

    # Apply distance filtering and edge margin
    valid = (depth_image > 0) & (depth_image < clip_distance_max)

    # Apply edge margin
    if edge_margin > 0:
        valid[:edge_margin // decimation_factor, :] = False
        valid[-edge_margin // decimation_factor:, :] = False
        valid[:, :edge_margin // decimation_factor] = False
        valid[:, -edge_margin // decimation_factor:] = False

    # Calculate 3D coordinates in depth camera space
    z = depth_image
    x = z * (c - depth_intrinsics.ppx) / depth_intrinsics.fx
    y = -z * (r - depth_intrinsics.ppy) / depth_intrinsics.fy  # Flip Y

    # Apply validity mask and flatten
    valid_mask = np.ravel(valid)
    z_valid = np.ravel(z)[valid_mask]
    x_valid = np.ravel(x)[valid_mask]
    y_valid = np.ravel(y)[valid_mask]

    # Manual reprojection to color camera
    # Transform points from depth camera space to color camera space
    rotation_matrix = np.array(extrinsics.rotation).reshape(3, 3)
    translation_vector = np.array(extrinsics.translation)

    # Apply transformation: P_color = R * P_depth + t
    points_depth = np.column_stack((x_valid, -y_valid, z_valid))  # Unflip Y for transformation
    points_color = points_depth @ rotation_matrix.T + translation_vector

    # Project to color image coordinates
    x_color, y_color, z_color = points_color[:, 0], points_color[:, 1], points_color[:, 2]

    # Avoid division by zero
    z_nonzero = np.where(z_color > 0, z_color, 1)
    u = (x_color * color_intrinsics.fx / z_nonzero + color_intrinsics.ppx).astype(int)
    v = (y_color * color_intrinsics.fy / z_nonzero + color_intrinsics.ppy).astype(int)

    # Check bounds for color image
    color_height, color_width = color_image.shape[:2]
    in_bounds = (u >= 0) & (u < color_width) & (v >= 0) & (v < color_height) & (z_color > 0)

    # Initialize colors
    r_color = np.zeros(len(x_valid), dtype=np.uint8)
    g_color = np.zeros(len(x_valid), dtype=np.uint8)
    b_color = np.zeros(len(x_valid), dtype=np.uint8)

    # Sample colors for points within bounds
    if np.any(in_bounds):
        r_color[in_bounds] = color_image[v[in_bounds], u[in_bounds], 0]
        g_color[in_bounds] = color_image[v[in_bounds], u[in_bounds], 1]
        b_color[in_bounds] = color_image[v[in_bounds], u[in_bounds], 2]

    # Detect pixels needing gradient (out of bounds OR genuinely black)
    needs_gradient = (~in_bounds)

    # Apply distance-based gradient for pixels needing it
    if np.any(needs_gradient):
        normalized_depth = np.clip(z_valid[needs_gradient] / clip_distance_max, 0.0, 1.0)

        # Hot-to-cold gradient: close=red, middle=yellow, far=blue
        gradient_r = np.where(normalized_depth < 0.5,
                              255,
                              (255 * (1.0 - normalized_depth) * 2).astype(np.uint8))
        gradient_g = np.where(normalized_depth < 0.5,
                              (255 * normalized_depth * 2).astype(np.uint8),
                              (255 * (1.0 - normalized_depth) * 2).astype(np.uint8))
        gradient_b = np.where(normalized_depth < 0.5,
                              0,
                              (255 * (normalized_depth - 0.5) * 2).astype(np.uint8))

        r_color[needs_gradient] = gradient_r
        g_color[needs_gradient] = gradient_g
        b_color[needs_gradient] = gradient_b

    # Combine into point cloud array
    pointcloud = np.column_stack((x_valid, y_valid, z_valid, r_color, g_color, b_color))

    return pointcloud


def get_camera_intrinsics_and_extrinsics(pipeline):
    """Extract camera intrinsics and extrinsics from RealSense pipeline"""
    profile = pipeline.get_active_profile()

    # Get streams
    color_stream = profile.get_stream(rs.stream.color)
    depth_stream = profile.get_stream(rs.stream.depth)

    # Get intrinsics
    color_intrinsics = color_stream.as_video_stream_profile().get_intrinsics()
    depth_intrinsics = depth_stream.as_video_stream_profile().get_intrinsics()

    # Get extrinsics (depth to color transformation)
    depth_to_color_extrinsics = depth_stream.get_extrinsics_to(color_stream)

    return color_intrinsics, depth_intrinsics, depth_to_color_extrinsics


def main():
    # Configuration
    node_name = "camera_streamer"
    service_port = "camera_1"

    # This streamer uses depth as primary data source, manually reprojects
    # to sample color, and fills missing areas with distance-based gradient

    comms_node = UDPVideoCommsNode(
        node_name=node_name,
        service_port=service_port,
        verbose=False,
        silent=False,
        chunk_size=1200,
        jpeg_quality=85,
        log_interval=5.0,
        intrinsics_interval=2.0,
        localhost_port=9090
    )

    # Initialise RealSense pipeline
    pipeline = rs.pipeline()
    config = rs.config()

    config.enable_stream(rs.stream.color, 640, 480, rs.format.rgb8, 30)
    config.enable_stream(rs.stream.depth, 640, 480, rs.format.z16, 30)

    # No alignment needed - we'll do manual reprojection

    pipeline.start(config)
    print("RealSense camera started")
    print("UDP streaming configured: Remote device + localhost:9001 for Unity Play Mode")

    # Get depth scale
    profile = pipeline.get_active_profile()
    depth_sensor = profile.get_device().first_depth_sensor()
    depth_scale = depth_sensor.get_depth_scale()
    print(f"Depth scale: {depth_scale}")

    # Extract and set camera intrinsics
    try:
        color_intrinsics, depth_intrinsics, depth_to_color_extrinsics = get_camera_intrinsics_and_extrinsics(pipeline)
        comms_node.set_camera_intrinsics(color_intrinsics, depth_intrinsics, depth_to_color_extrinsics)

        print("Camera intrinsics extracted:")
        print(f"RGB: fx={color_intrinsics.fx:.2f}, fy={color_intrinsics.fy:.2f}, "
              f"ppx={color_intrinsics.ppx:.2f}, ppy={color_intrinsics.ppy:.2f}")
        print(f"Depth: fx={depth_intrinsics.fx:.2f}, fy={depth_intrinsics.fy:.2f}, "
              f"ppx={depth_intrinsics.ppx:.2f}, ppy={depth_intrinsics.ppy:.2f}")
        print(f"Extrinsics translation: {depth_to_color_extrinsics.translation}")

    except Exception as e:
        print(f"Failed to extract camera intrinsics: {e}")

    print("Waiting for Quest connection...")
    while True:
        ui_connected, quest_connected = comms_node.is_connected()
        if quest_connected:
            print("Quest connected! Streaming point clouds...")
            break
        time.sleep(1.0)

    # Point cloud parameters
    clip_distance_max = 2.0  # Reduce max distance from 3.5m to 2.0m
    edge_margin = 20  # Increase edge margin from 5 to 20 pixels
    decimation_factor = 2  # Add decimation - only process every Nth pixel

    # Decoupled frame rates - RGB at full camera rate (~30fps), point clouds at reduced rate
    pointcloud_target_fps = 30  # Send point clouds at 10 FPS (adjust as needed: 5-15 fps)
    pointcloud_interval = 1.0 / pointcloud_target_fps  # Time between point cloud sends

    frame_count = 0
    rgb_frame_count = 0
    pointcloud_frame_count = 0
    last_stats_time = time.time()
    last_pointcloud_time = 0

    while True:
        try:
            ui_connected, quest_connected = comms_node.is_connected()
            if not quest_connected:
                print("Quest disconnected, waiting for reconnection...")
                time.sleep(1.0)
                continue

            frames = pipeline.wait_for_frames()

            # Get raw frames (no alignment - we do manual reprojection)
            color_frame = frames.get_color_frame()
            depth_frame = frames.get_depth_frame()

            if not color_frame or not depth_frame:
                continue

            # Get RGB array for sending (raw color frame)
            rgb_array = np.asanyarray(color_frame.get_data())

            # Send RGB frame every frame (30 FPS)
            comms_node.send_rgb_frame(rgb_array)
            rgb_frame_count += 1

            # Send point cloud at reduced rate
            current_time = time.time()
            should_send_pointcloud = (current_time - last_pointcloud_time) >= pointcloud_interval

            if should_send_pointcloud:
                # Calculate point cloud with manual reprojection
                pointcloud_start = time.time()
                pointcloud = depth_to_pointcloud(
                    depth_frame,
                    color_frame,
                    depth_scale,
                    depth_intrinsics,
                    color_intrinsics,
                    depth_to_color_extrinsics,
                    clip_distance_max,
                    edge_margin,
                    decimation_factor
                )
                pointcloud_time = (time.time() - pointcloud_start) * 1000

                # Send point cloud
                comms_node.send_pointcloud_frame(pointcloud)
                pointcloud_frame_count += 1
                last_pointcloud_time = current_time
            else:
                pointcloud_time = 0

            # Simple frame rate calculation
            frame_count += 1
            current_time = time.time()
            if current_time - last_stats_time >= 5.0:
                elapsed = current_time - last_stats_time
                total_fps = frame_count / elapsed
                rgb_fps = rgb_frame_count / elapsed
                pointcloud_fps = pointcloud_frame_count / elapsed
                avg_points = len(pointcloud) if 'pointcloud' in locals() and len(pointcloud) > 0 else 0
                print(f"Total FPS: {total_fps:.1f}, RGB FPS: {rgb_fps:.1f}, PointCloud FPS: {pointcloud_fps:.1f}")
                print(f"Points: {avg_points}, PointCloud calc: {pointcloud_time:.1f}ms (manual reprojection)")
                frame_count = 0
                rgb_frame_count = 0
                pointcloud_frame_count = 0
                last_stats_time = current_time

            # Handle actions and config changes
            actions = comms_node.get_actions()
            for action in actions:
                pass

            config_changed, current_config = comms_node.get_latest_config()
            if config_changed:
                pass

            time.sleep(0.001)

        except Exception as e:
            print(f"Streaming error: {e}")
            time.sleep(0.1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
