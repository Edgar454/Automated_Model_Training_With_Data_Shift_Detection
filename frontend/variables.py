GLASS_CSS = """
<style>
:root{
    --glass-bg: rgba(255,255,255,0.08);
    --glass-border: rgba(255,255,255,0.12);
    --glass-blur: 10px;
    --card-radius: 16px;
    --accent: rgba(66,139,255,0.95);
}
body, .main, .block-container {
    background: linear-gradient(180deg, #e6f0ff 0%, #f6f0f8 100%);
}
.glass {
    background: var(--glass-bg);
    border: 1px solid var(--glass-border);
    backdrop-filter: blur(var(--glass-blur));
    -webkit-backdrop-filter: blur(var(--glass-blur));
    border-radius: var(--card-radius);
    padding: 18px;
    color: #0b1a2b;
    box-shadow: 0 8px 30px rgba(8,20,40,0.08);
}
.header {
    text-align: center;
    padding: 18px;
}
.title {
    font-size: 42px;
    font-weight: 700;
    margin-bottom: 4px;
    color: #0b2a4a;
    letter-spacing: -0.5px;
}
.subtitle {
    color: #234a6a;
    opacity: 0.9;
    margin-bottom: 16px;
}
.card-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 14px;
}
.small-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
}
.big-temp {
    display:flex;
    align-items:center;
    gap: 18px;
}
.big-temp .temp {
    font-size: 56px;
    font-weight:700;
    color:#06263f;
}
.kpi {
    background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
    padding: 10px;
    border-radius: 12px;
    text-align:center;
}
.small-kpi {
    padding: 8px;
    border-radius: 10px;
    text-align:center;
    font-weight:600;
}
.footer {
    text-align:center;
    font-size:13px;
    color:#315a7f;
    opacity:0.8;
    margin-top:18px;
}
button.stButton>button {
    background: linear-gradient(90deg, #3b82f6 0%, #60a5fa 100%);
    color: white;
    border-radius: 10px;
    border: none;
    padding: 8px 16px;
}
</style>
"""