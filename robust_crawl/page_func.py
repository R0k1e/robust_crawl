import random
import numpy as np
import logging

logger = logging.getLogger(__name__)


def simulate_mouse_movements(page, element, jitter=3):
    bounding_box = element.bounding_box()
    window_size = page.viewport_size
    start_x, start_y = get_random_position_within_window(page)
    end_x, end_y = get_random_position_within_element(bounding_box, window_size)

    # Calculate the distance between the start and end points
    distance = np.sqrt((end_x - start_x) ** 2 + (end_y - start_y) ** 2)
    num_points = int(distance / 10)

    control1 = (
        start_x + random.randint(-50, 50),
        start_y + random.randint(-50, 50),
    )
    control2 = (end_x + random.randint(-50, 50), end_y + random.randint(-50, 50))

    curve_points = bezier_curve(
        (start_x, start_y), control1, control2, (end_x, end_y), num_points, jitter
    )

    for point in curve_points:
        target_x = max(1, min(point[0], window_size["width"] - 1))
        target_y = max(1, min(point[1], window_size["height"] - 1))
        page.mouse.move(target_x, target_y)
        page.wait_for_timeout(random.uniform(10, 50))
        start_x, start_y = target_x, target_y

    page.wait_for_timeout(random.uniform(500, 1500))
    # remove_debug_markers()
    return target_x, target_y


def simulate_box_input(page, search_box, text):
    search_box.hover()
    search_box.click()

    current_text = search_box.input_value()
    search_box.press("End")
    for _ in range(len(current_text) + random.randint(1, 5)):
        search_box.press("Backspace")
        page.wait_for_timeout(max(0, random.gauss(15, 20)))

    page.wait_for_timeout(300)

    for char in text:
        if random.random() < 0.05:
            wrong_char = random.choice("abcdefghijklmnopqrstuvwxyz")
            search_box.type(wrong_char)
            page.wait_for_timeout(max(0, random.gauss(150, 50)))
            search_box.press("Backspace")
            page.wait_for_timeout(max(0, random.gauss(150, 50)))

        search_box.type(char)
        page.wait_for_timeout(max(0, random.gauss(150, 50)))
    search_box.press("Enter")


def browse_random_context(page):
    urls = [
        "https://www.wikipedia.org",
        "https://www.github.com",
        "https://www.stackoverflow.com",
        "https://developer.mozilla.org",
    ]
    url = random.choice(urls)
    try:
        page.goto(url)
        logger.debug(f"Browsing random context: {url}")
    except Exception as e:
        logger.error(f"Context load failed for URL: {url}. Error: {e}")


def scroll_element_into_view(page, element):
    element.scroll_into_view_if_needed()
    page.wait_for_timeout(100)


def get_random_position_within_window(page):
    viewport_size = page.viewport_size
    return random.randint(0, viewport_size["width"]), random.randint(
        0, viewport_size["height"]
    )


def bezier_curve(start, control1, control2, end, num_points, jitter):
    points = []
    for i in range(num_points):
        t = i / num_points
        x = (
            (1 - t) ** 3 * start[0]
            + 3 * (1 - t) ** 2 * t * control1[0]
            + 3 * (1 - t) * t**2 * control2[0]
            + t**3 * end[0]
        )
        y = (
            (1 - t) ** 3 * start[1]
            + 3 * (1 - t) ** 2 * t * control1[1]
            + 3 * (1 - t) * t**2 * control2[1]
            + t**3 * end[1]
        )
        x += random.uniform(-jitter, jitter)
        y += random.uniform(-jitter, jitter)
        points.append((x, y))
    return points


def get_random_position_within_element(bounding_box, window_size):
    end_x = min(
        window_size["width"] - 1,
        bounding_box["x"] + random.uniform(0, bounding_box["width"]),
    )
    end_y = min(
        window_size["height"] - 1,
        bounding_box["y"] + random.uniform(0, bounding_box["height"]),
    )
    return end_x, end_y


def draw_dot(page, x, y):
    page.evaluate(
        """
        var dot = document.createElement('div');
        dot.style.width = '5px';
        dot.style.height = '5px';
        dot.style.backgroundColor = 'red';
        dot.style.position = 'absolute';
        dot.style.left = arguments[0] + 'px';
        dot.style.top = arguments[1] + 'px';
        dot.style.zIndex = 10000;
        document.body.appendChild(dot);
    """,
        x,
        y,
    )


def remove_debug_markers(page):
    page.evaluate(
        """
        var markers = document.querySelectorAll('div[style*="background-color: red;"]');
        markers.forEach(function(marker) {
            marker.parentNode.removeChild(marker);
        });
    """
    )
