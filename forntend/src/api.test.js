import { describe, expect, it } from "vitest";
import { formatMoney, toMinorBigInt } from "./api.js";

describe("minor-unit money helpers", () => {
  it("formats cents without floating point arithmetic", () => {
    expect(formatMoney(12345, "USD")).toBe("$123.45");
    expect(formatMoney("100", "EUR")).toMatch(/1[.,]00/);
  });

  it("keeps large integer values exact and rejects unsafe numeric balances", () => {
    expect(formatMoney(9007199254740991n, "USD")).toContain("90,071,992,547,409.91");
    expect(toMinorBigInt(Number.MAX_SAFE_INTEGER + 2)).toBeNull();
    expect(formatMoney(Number.MAX_SAFE_INTEGER + 2, "USD")).toBe("—");
  });
});
