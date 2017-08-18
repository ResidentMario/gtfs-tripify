def synthesize_route(station_lists):
    """
    Given a list of station lists (that is: a list of lists, where each sublist consists of the series of stations
    which a train was purported to be heading towards at any one time), returns the synthetic route of all of the
    stops that train may have stopped at, in the order in which those stops would have occurred.
    """
    ret = []
    for i in range(len(station_lists)):
        ret = _synthesize_station_lists(ret, station_lists[i])
    return ret


def _synthesize_station_lists(left, right):
    """
    Pairwise synthesis op. Submethod of the above.
    """
    # First, find the pivot.
    pivot_left = pivot_right = -1
    for j in range(len(left)):
        station_a = left[j]
        for k in range(len(right)):
            station_b = right[k]
            if station_a == station_b:
                pivot_left = j
                pivot_right = k
                break

    # If we found a pivot...
    if pivot_left != -1:
        # ...then the stations that appear before the pivot in the first list, the pivot, and the stations that
        # appear after the pivot in the second list should be the ones that are included
        return (left[:pivot_left] +
                [s for s in right[:pivot_right] if s not in left[:pivot_left]] +
                right[pivot_right:])
    # If we did not find a pivot...
    else:
        # ...then none of the stations that appear in the second list appeared in the first list. This means that the
        #  train probably cancelled those stations, but it may have stopped there in the meantime also. Add all
        # stations in the first list and all stations in the second list together.
        return left + right
