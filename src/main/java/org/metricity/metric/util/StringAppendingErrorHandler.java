package org.metricity.metric.util;

import java.util.function.Consumer;

public class StringAppendingErrorHandler implements Consumer<String> {
	private final String theDelimiter;
	private final StringBuilder theText;

	public StringAppendingErrorHandler(String delimiter) {
		theDelimiter = delimiter;
		theText = new StringBuilder();
	}

	public StringBuilder getText() {
		return theText;
	}

	public boolean isEmpty() {
		return theText.length() == 0;
	}

	public StringAppendingErrorHandler borderWith(String prepend, String postpend) {
		theText.insert(0, prepend);
		theText.append(postpend);
		return this;
	}

	@Override
	public void accept(String s) {
		if (s == null || s.length() == 0) {
			return;
		} else if (theText.length() > 0) {
			theText.append(theDelimiter);
		}
		theText.append(s);
	}

	private void append(String text, String... prepend) {
		if (theText.length() > 0) {
			theText.append(theDelimiter);
		}
		for (String s : prepend) {
			theText.append(s);
		}
		theText.append(text);
	}

	@Override
	public String toString() {
		return theText.toString();
	}

	public Consumer<String> withPrepend(String... prepend) {
		return err -> {
			StringAppendingErrorHandler.this.append(err, prepend);
		};
	}
}
