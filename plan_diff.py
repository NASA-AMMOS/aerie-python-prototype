def diff(old_directives, new_directives):
    old_directives = list(old_directives)
    new_directives = list(new_directives)
    unchanged_directives = []

    any_matched = True
    while any_matched:
        # TODO use a more efficient diff algorithm
        any_matched = False
        for old in old_directives:
            for new in new_directives:
                if old == new:
                    unchanged_directives.append(old)
                    new_directives.remove(new)
                    old_directives.remove(old)
                    any_matched = True
    deleted_directives = old_directives
    added_directives = new_directives
    return unchanged_directives, deleted_directives, added_directives
