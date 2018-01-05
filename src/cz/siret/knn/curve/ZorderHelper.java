package cz.siret.knn.curve;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.Random;

import cz.siret.knn.model.Feature;

public class ZorderHelper {

	private static final int COORD_SIZE = 32;

	private static void reverse(byte[] arr) {
		int length = arr.length;
		for (int i = 0; i < length / 2; i++) {
			byte b = arr[i];
			arr[i] = arr[length - i - 1];
			arr[length - i - 1] = b;
		}
	}

	public static int[] createShift(int dimension, Random r, boolean shift, int shiftRange) {
		int[] rv = new int[dimension]; // random vector

		if (shift) {
			for (int i = 0; i < dimension; i++) {
				rv[i] = Math.abs(r.nextInt(shiftRange));
			}
		} else {
			for (int i = 0; i < dimension; i++)
				rv[i] = 0;
		}

		return rv;
	}

	public static BigInteger valueOf(int[] coords) {
		if (coords == null)
			throw new NullPointerException();
		int dim = coords.length;

		BitSet bitSet = new BitSet(dim * COORD_SIZE);
		for (int bitIndex = 0; bitIndex < COORD_SIZE; bitIndex++)
			for (int coordIndex = 0; coordIndex < dim; coordIndex++) {
				boolean bit = getBit(coords[coordIndex], bitIndex);
				bitSet.set(bitIndex * dim + (dim - coordIndex - 1), bit);
			}

		byte[] byteVal = bitSet.toByteArray();
		reverse(byteVal);
		return new BigInteger(1, byteVal);
	}

	public static void toCoords(BigInteger zval, int[] coordsOutput) {
		if (zval == null || coordsOutput == null)
			throw new NullPointerException();

		int dim = coordsOutput.length;
		for (int i = 0; i < dim; i++)
			coordsOutput[i] = 0;

		for (int bitIndex = 0; bitIndex < COORD_SIZE; bitIndex++)
			for (int coordIndex = 0; coordIndex < dim; coordIndex++)
				if (zval.testBit(bitIndex * dim + (dim - coordIndex - 1))) {
					coordsOutput[coordIndex] = setBit(coordsOutput[coordIndex], bitIndex);
				}
	}

	public static BigInteger maxValue(int dim) {
		BitSet bitSet = new BitSet(dim * COORD_SIZE);
		bitSet.set(0, dim * COORD_SIZE);
		byte[] byteVal = bitSet.toByteArray();
		reverse(byteVal);
		return new BigInteger(1, byteVal);
	}

	public static BigInteger minValue() {
		return BigInteger.ZERO;
	}

	private static boolean getBit(int number, int index) {
		number &= 1 << index;
		return number != 0;
	}

	private static int setBit(int number, int index) {
		return number | (1 << index);
	}

	public static void main(String[] args) throws IOException {

		if (args.length != 2)
			throw new Error("usage: Zorder <dim> <scale>  < FeatureWritable");
		int dim = Integer.parseInt(args[0]);
		int scale = Integer.parseInt(args[1]);

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		Feature fw = Feature.parse(in.readLine().split(",")[1]);
		int[] arr = new int[dim];
		for (Feature.DimValue dv : fw.iterate(dim))
			arr[dv.dim] = (int) (dv.value * scale);

		System.out.println(ZorderHelper.valueOf(arr));
	}
}
