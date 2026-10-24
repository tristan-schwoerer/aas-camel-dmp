/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.jupiter.api.Test;

/** Int64 and UInt64 writes carry the value Lua returned, or fail: never a saturated or wrapped one. */
class DmpMiloWriteProducerRangeTest {
	@Test
	void int64TakesEveryLongAndNothingBeyond() {
		assertEquals(Long.MAX_VALUE, DmpMiloWriteProducer.exactLong(BigInteger.valueOf(Long.MAX_VALUE)));
		assertEquals(-42L, DmpMiloWriteProducer.exactLong(-42.0));
		assertEquals(7L, DmpMiloWriteProducer.exactLong(new BigDecimal("7.000")));
		assertThrows(ArithmeticException.class, () -> DmpMiloWriteProducer.exactLong(1e20), "a double does not saturate");
		assertThrows(ArithmeticException.class, () -> DmpMiloWriteProducer.exactLong(0x1p63));
		assertThrows(ArithmeticException.class,
				() -> DmpMiloWriteProducer.exactLong(BigInteger.ONE.shiftLeft(64).add(BigInteger.ONE)), "a BigInteger does not wrap");
	}

	@Test
	void uint64TakesTheWholeUnsignedRange() {
		BigInteger max = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
		assertEquals(max, DmpMiloWriteProducer.unsigned64(max), "values above Long.MAX_VALUE are valid UInt64");
		assertEquals(BigInteger.valueOf(5), DmpMiloWriteProducer.unsigned64(5L));
		assertThrows(ArithmeticException.class, () -> DmpMiloWriteProducer.unsigned64(-1L));
		assertThrows(ArithmeticException.class, () -> DmpMiloWriteProducer.unsigned64(max.add(BigInteger.ONE)));
	}
}
