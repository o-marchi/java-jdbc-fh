import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Principal {

	private static Connection createConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:postgresql://localhost:5432/javatest", "postgres", "postgres");
	}

	private static int insertCadastro(Connection conn, String name) throws SQLException {
		String sql = "INSERT INTO PUBLIC.CADASTRO (ID, NOME) VALUES ((SELECT COALESCE(MAX(ID), 0) + 1  FROM PUBLIC.CADASTRO), ?)";

		try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

			stmt.setString(1, name);
			stmt.executeUpdate();

			try (ResultSet result = stmt.getGeneratedKeys()) {
				result.next();
				return result.getInt(1);
			}
		}
	}

	private static void insertHistorico(Connection conn, int cadastroId, String value) throws SQLException {
		String sql = "INSERT INTO PUBLIC.HISTORICO (ID_CADASTRO, VALOR) VALUES (?, ?)";

		try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			stmt.setInt(1, cadastroId);
			stmt.setString(2, value);
			stmt.executeUpdate();
		}
	}

	private static void logCadastros(Connection conn) throws SQLException {

		String sql = "SELECT * FROM PUBLIC.CADASTRO\r\n"
				+ "INNER JOIN PUBLIC.HISTORICO ON CADASTRO.ID = HISTORICO.ID_CADASTRO\r\n";

		try (
			PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			ResultSet result = stmt.executeQuery();
		) {
			while (result.next()) {
				System.out.println(String.format("ID CADASTRO: %s | NOME: %s | ID HISTORICO: %s | VALOR: %s",
						result.getString("ID"), result.getString("NOME"), result.getString("ID_CADASTRO"),
						result.getString("VALOR")));
			}
		}

	}

	public static void main(String[] args) throws SQLException {

		String nome = args[0];
		String valor = args[1];

		Connection conn = Principal.createConnection();

		int cadastroId = Principal.insertCadastro(conn, nome);
		Principal.insertHistorico(conn, cadastroId, valor);

		Principal.logCadastros(conn);

		conn.close();
	}

}
