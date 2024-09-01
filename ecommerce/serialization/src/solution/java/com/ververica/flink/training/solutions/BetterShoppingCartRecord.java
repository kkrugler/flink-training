package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.BaseShoppingCartRecord;
import com.ververica.flink.training.common.CartItem;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class BetterShoppingCartRecord extends BaseShoppingCartRecord {

    @TypeInfo(ListInfoFactory.class)
    private List<CartItem> items;

    public BetterShoppingCartRecord() {
        super();
    }

    public BetterShoppingCartRecord(BetterShoppingCartRecord clone) {
        super(clone);
    }

    public List<CartItem> getItems() {
        return items;
    }

    public void setItems(List<CartItem> items) {
        this.items = items;
    }

    public static class ListInfoFactory<E> extends TypeInfoFactory<List<E>> {

        @Override
        public TypeInformation<List<E>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            TypeInformation<?> elementType = genericParameters.get("E");
            if (elementType == null) {
                throw new InvalidTypesException("Type extraction is not possible on List (element type unknown).");
            }

            return Types.LIST((TypeInformation<E>) elementType);
        }
    }

}
