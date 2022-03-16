package mapreduce;

import java.util.function.Function;

public enum AirlineCol {
    Year(0),
    Month(1),
    DayofMonth(2),
    DayOfWeek(3),
    DepTime(4),
    CRSDepTime(5),
    ArrTime(6),
    CRSArrTime(7),
    UniqueCarrier(8),
    FlightNum(9),
    TailNum(10),
    ActualElapsedTime(11),
    CRSElapsedTime(12),
    AirTime(13),
    ArrDelay(14),
    DepDelay(15),
    Origin(16),
    Dest(17),
    Distance(18),
    TaxiIn(19),
    TaxiOut(20),
    Cancelled(21),
    CancellationCode(22),
    Diverted(23),
    CarrierDelay(24),
    WeatherDelay(25),
    NASDelay(26),
    SecurityDelay(27),
    LateAircraftDelay(28);

    public final int index;
    AirlineCol(int index) {
        this.index = index;
    }

    // 不把Function接口直接暴露出去
    public static class ColGetter {
        private final Function<AirlineCol, String> fn;

        private ColGetter(Function<AirlineCol, String> fn) {
            this.fn = fn;
        }
        public String apply(AirlineCol colEnum) {
            return fn.apply(colEnum);
        }
    }

    public static ColGetter build(String[] cols) {
        return new ColGetter((col) -> cols[col.index]);
    }
}