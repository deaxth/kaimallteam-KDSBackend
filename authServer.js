require('dotenv').config();

const express = require('express');
const app = express();
const port = process.env.AUTH_PORT;

const jwt = require('jsonwebtoken');

const sql = require('mssql');
const { getSQLPool } = require('./mssql-pool-management');
const config = require('./config/localConfig');

app.use(express.json());

function handleInternalError(err, res) {
  console.log(err);
  res.status(500).json({ message: 'Something went wrong. Please contact support' });
}

async function authenticateCredentials(req, res, next) {
  const { userName, password } = req.body;
  const pool = await getSQLPool(config);
  try {
    const getUser = await pool.request()
      .input('UserName', sql.VarChar, userName)
      .input('Password', sql.VarChar, password)
      .query(`SELECT u.UserID, t.Token FROM UsersInternalApps u
        LEFT JOIN RefreshTokens t 
        ON u.UserID = t.UserID
        WHERE u.LoginName = @UserName AND u.Password = @Password;`);
    const userId = getUser.recordset[0]?.UserID;
    const token = getUser.recordset[0]?.Token;
    let newToken = '';
    if (!userId) return res.status(404).json({ message: 'User does not exist.' });

    if (!token) {
      const refreshToken = jwt.sign({userId: userId}, process.env.REFRESH_SECRET);
      const getNewToken = await pool.request()
        .input('Token', sql.VarChar, refreshToken)
        .input('UserID', sql.Int, userId)
        .query(`INSERT INTO RefreshTokens (Token, UserID, DateCreated) OUTPUT INSERTED.Token VALUES (@Token, @UserID, GETDATE())`);

      newToken = getNewToken.recordset[0].Token;
    }
    req.userId = userId;
    req.refreshToken = token ?? newToken;
    next();
  } catch(err) {
    handleInternalError(err, res);
  }
}

function generateAuthToken(user) {
  return jwt.sign(user, process.env.AUTH_SECRET, { expiresIn: '15s' });
}

function authenticateToken(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ message: 'Missing token.' });
  const [, token] = authHeader.split(' ');

  jwt.verify(token, process.env.AUTH_SECRET, (err, user) => {
    console.log(err);
    if (err) return res.status(400).json({ message: 'Invalid or expired token.' });
    req.user = user;
    next();
  })
}

const items = [
  {
    userId: 1,
    item: 'Sinigang'
  },
  {
    userId: 2,
    item: 'Bulalo'
  },
  {
    userId: 1,
    item: 'Pork Sisig'
  }
];

app.get('/items', authenticateToken, (req, res) => {
  const userId = req.user.userId;

  const item = items.filter(item => userId === item.userId);

  res.json({ items: item });
});

app.get('/renew-token', async(req, res) => {
  const authHeader = req.headers.authorization;
  const [, authToken] = authHeader.split(" ");
  const token = authToken;

  const pool = await getSQLPool(config);
  try {
    const checkToken = await pool.request()
      .input('Token', sql.VarChar, authToken)
      .query('SELECT RecordID FROM RefreshTokens WHERE Token = @Token');
    console.log(checkToken)
    const dbtoken = checkToken?.recordset[0]?.RecordID;

    if (!dbtoken) return res.status(401).json({ message: 'Refresh token does not exist.' });

    jwt.verify(token, process.env.REFRESH_SECRET, (err, user) => {
      if (err) return res.sendStatus(403);
      console.log(user)
      const accessToken = generateAuthToken({userId: user.userId});
      res.json({ token: accessToken });
    })
  } catch(err) {
    handleInternalError(err, res);
  }
})

app.post('/login', authenticateCredentials, (req, res) => {
  const userId = req.userId;
  const refreshToken = req.refreshToken;
  const user = { userId: userId };

  const accessToken = generateAuthToken(user);

  res.json({ accessToken: accessToken, refreshToken: refreshToken });
});

app.post('/logout', authenticateToken, async(req, res) => {
  const userId = req.user.userId;
  const pool = await getSQLPool(config);
  try {
    await pool.request()
      .input('UserID', sql.Int, userId)
      .query('DELETE FROM RefreshTokens WHERE UserID = @UserID');

    res.json({ message: 'Logout successfully.' });
  } catch(err) {
    handleInternalError(err, res);
  }
})


app.listen(port, () => console.log(`KDS auth server running on port ${port}`));