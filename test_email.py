from src.email_sender import send_weekly_insights

send_weekly_insights(
    client_name="Test",
    client_email="melissa.c.castaneda.p@gmail.com",
    insights_text="## Test\n\nEsto es una prueba del pipeline de Clearpath.\n\n**Si ves este email, el sistema funciona correctamente.**"
)