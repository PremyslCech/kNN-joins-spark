package cz.siret.knn.model;

@SuppressWarnings("serial")
public class NNResultWithError extends NNResult {

	private final String errorMessage;

	public NNResultWithError(String errorMessage) {
		this.errorMessage = errorMessage;

	}

	@Override
	public String toString() {
		return errorMessage;
	}
}
