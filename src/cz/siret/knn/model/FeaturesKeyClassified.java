package cz.siret.knn.model;

@SuppressWarnings("serial")
public class FeaturesKeyClassified extends FeaturesKey {

    private int classification;

    public FeaturesKeyClassified(int fiveMinuteId, int clientId, int classification) {
        super(fiveMinuteId, clientId);
        this.classification = classification;
    }

    public FeaturesKeyClassified(FeaturesKeyClassified fk) {
        super(fk);
        classification = fk.classification;
    }

    public FeaturesKeyClassified() {
        super();
    }

    public int getClassification() {
        return classification;
    }

    public void setClassification(int classification) {
        this.classification = classification;
    }

    @Override
    public String toString() {
        return super.toString()+":"+classification;
    }

    @Override
    public FeaturesKeyClassified clone() {
        return new FeaturesKeyClassified(getFiveMinuteId(), getClientId(), classification);
    }

    public static FeaturesKeyClassified parse(String s) {
        String[] split = s.split(":");
        FeaturesKey fkw = FeaturesKey.parse(split[0]);
        return new FeaturesKeyClassified(fkw.getFiveMinuteId(), fkw.getClientId(), Integer.valueOf(split[1]));
    }
}
