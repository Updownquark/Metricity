package org.metricity.anchor;

import org.observe.util.TypeTokens;
import org.qommons.StringUtils;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

public interface AnchorType<A extends Anchor> extends Comparable<AnchorType<?>> {
	@SuppressWarnings("rawtypes")
	static TypeTokens.TypeKey<AnchorType> TYPE_KEY = TypeTokens.get().keyFor(AnchorType.class)
		.enableCompoundTypes(new TypeTokens.UnaryCompoundTypeCreator<AnchorType>() {
			@Override
			public <P> TypeToken<? extends AnchorType> createCompoundType(TypeToken<P> param) {
				return createAnchorType((TypeToken<? extends Anchor>) param);
			}

			private <A extends Anchor> TypeToken<AnchorType<A>> createAnchorType(TypeToken<A> param) {
				return new TypeToken<AnchorType<A>>() {}.where(new TypeParameter<A>() {}, param);
			}
		});
	static TypeToken<AnchorType<?>> TYPE = TYPE_KEY.parameterized();

	TypeToken<A> getType();

	String getName();

	@Override
	default int compareTo(AnchorType<?> o) {
		if (this == o)
			return 0;
		return StringUtils.compareNumberTolerant(getName(), o.getName(), true, true);
	}
}
