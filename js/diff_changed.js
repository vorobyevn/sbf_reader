function diff_changed(a, b) {

	if (a === b) {
		return { changed: 'equal', value: a }
	}

	var value = {};
	var equal = true;
	for (var key in a) {
		if (key in b) {
			if (a[key] === b[key]) {
			} else {
				equal = false;
				value[key] = { changed: 'change', removed: a[key], added: b[key] }
			}
		} else {
			equal = false;
			value[key] = { changed: 'removed', value: a[key] }
		}
	}

	for (key in b) {
		if (!(key in a)) {
			equal = false;
			value[key] = { changed: 'added', value: b[key] }
		}
	}

	if (equal) {
		return { changed: 'equal', value: a }
	} else {
		return { changed: 'change', value: value }
	}
};